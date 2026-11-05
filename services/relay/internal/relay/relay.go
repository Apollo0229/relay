// Package relay moves messages from the Postgres outbox to Kafka.
//
// Each round claims a batch of unpublished messages, publishes them, and marks
// the ones Kafka acknowledged in the same transaction that claimed them.
// Delivery is at least once: if the relay dies after Kafka acknowledges a
// batch but before the commit, the batch is published again on restart, and
// consumers drop the copies by event id.
package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Message is one outbox row on its way to Kafka.
type Message struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte
}

// Failure records why a message could not be published.
type Failure struct {
	ID  int64
	Err error
}

// Store hands out batches of unpublished messages.
type Store interface {
	// Claim locks up to limit unpublished messages, oldest first, until the
	// batch is settled. An empty batch must still be settled.
	Claim(ctx context.Context, limit int) (Batch, error)
}

// Batch is a set of claimed messages awaiting their outcome.
type Batch interface {
	Messages() []Message
	// Settle marks published messages, records failures and releases the claim.
	Settle(ctx context.Context, published []int64, failed []Failure) error
}

// Publisher sends messages to the broker and reports one error per message,
// in the order given; nil means the broker acknowledged it.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) []error
}

// Relay drains the outbox into the publisher.
type Relay struct {
	Store     Store
	Publisher Publisher
	Logger    *slog.Logger

	// BatchSize caps the messages claimed per round.
	BatchSize int
	// PollInterval is the longest the relay waits between rounds when idle.
	PollInterval time.Duration
	// Wake, if set, cuts the wait short as soon as new messages are written.
	Wake <-chan struct{}
	// MaxBackoff caps the delay after a failed round; it doubles from PollInterval / 10.
	MaxBackoff time.Duration
}

// Result summarizes one round.
type Result struct {
	Claimed   int
	Published int
	Failed    int
}

// RunOnce claims, publishes and settles a single batch.
func (r *Relay) RunOnce(ctx context.Context) (Result, error) {
	batch, err := r.Store.Claim(ctx, r.BatchSize)
	if err != nil {
		return Result{}, err
	}
	msgs := batch.Messages()
	if len(msgs) == 0 {
		return Result{}, batch.Settle(ctx, nil, nil)
	}

	errs := r.Publisher.Publish(ctx, msgs)
	if len(errs) != len(msgs) {
		errs = make([]error, len(msgs))
		for i := range errs {
			errs[i] = errors.New("publisher returned a mismatched result count")
		}
	}
	published, failed := settle(msgs, errs)

	// Settle on a fresh context so a shutdown mid-publish still records what Kafka accepted.
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := batch.Settle(settleCtx, published, failed); err != nil {
		return Result{}, err
	}
	return Result{Claimed: len(msgs), Published: len(published), Failed: len(failed)}, nil
}

// Run publishes until ctx is cancelled. It returns nil on a clean shutdown.
func (r *Relay) Run(ctx context.Context) error {
	log := r.Logger
	if log == nil {
		log = slog.Default()
	}
	minBackoff := max(r.PollInterval/10, 10*time.Millisecond)
	maxBackoff := max(r.MaxBackoff, minBackoff)
	backoff := minBackoff

	for {
		res, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}

		switch {
		case err != nil:
			log.Error("relay round failed", "err", err, "retry_in", backoff)
		case res.Failed > 0:
			log.Warn("some messages were not published", "published", res.Published, "failed", res.Failed, "retry_in", backoff)
		default:
			if res.Published > 0 {
				log.Info("published", "messages", res.Published)
			}
			backoff = minBackoff
			if res.Claimed == r.BatchSize {
				continue // more may be waiting
			}
			if !r.wait(ctx, r.PollInterval, r.Wake) {
				return nil
			}
			continue
		}

		// A failed round waits out the backoff, ignoring wake-ups that would retry too soon.
		if !r.wait(ctx, backoff, nil) {
			return nil
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// wait sleeps for d, or until wake fires. It reports false if ctx ended first.
func (r *Relay) wait(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	case <-wake:
	}
	return true
}

// settle splits a batch into messages to mark published and messages to retry.
//
// Once a message fails, later messages with the same key are retried too, even
// if the broker accepted them. Their published_at never runs ahead of an
// earlier failure, so a retry replays that key's messages in their original
// order; the extra copies are dropped by consumers as duplicates.
func settle(msgs []Message, errs []error) (published []int64, failed []Failure) {
	blocked := map[string]error{}
	for i, m := range msgs {
		err := errs[i]
		if err == nil {
			if cause, ok := blocked[m.Key]; ok {
				err = &blockedError{cause: cause}
			}
		} else if _, ok := blocked[m.Key]; !ok {
			blocked[m.Key] = err
		}

		if err == nil {
			published = append(published, m.ID)
		} else {
			failed = append(failed, Failure{ID: m.ID, Err: err})
		}
	}
	return published, failed
}

type blockedError struct{ cause error }

func (e *blockedError) Error() string {
	return "held back behind an earlier failure for the same key: " + e.cause.Error()
}

func (e *blockedError) Unwrap() error { return e.cause }
