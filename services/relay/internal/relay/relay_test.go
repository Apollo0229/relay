package relay

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// memStore is an in-memory outbox with the same claim/settle contract as Postgres.
type memStore struct {
	mu      sync.Mutex
	pending []Message
	sent    map[int64]bool
	errors  map[int64]string
	claims  int
}

func newMemStore(msgs ...Message) *memStore {
	return &memStore{pending: msgs, sent: map[int64]bool{}, errors: map[int64]string{}}
}

func (s *memStore) Claim(_ context.Context, limit int) (Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims++
	var out []Message
	for _, m := range s.pending {
		if !s.sent[m.ID] && len(out) < limit {
			out = append(out, m)
		}
	}
	return &memBatch{store: s, msgs: out}, nil
}

func (s *memStore) add(msgs ...Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, msgs...)
}

func (s *memStore) published() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []int64
	for _, m := range s.pending {
		if s.sent[m.ID] {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

type memBatch struct {
	store   *memStore
	msgs    []Message
	settled bool
}

func (b *memBatch) Messages() []Message { return b.msgs }

func (b *memBatch) Settle(ctx context.Context, published []int64, failed []Failure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	for _, id := range published {
		b.store.sent[id] = true
	}
	for _, f := range failed {
		b.store.errors[f.ID] = f.Err.Error()
	}
	b.settled = true
	return nil
}

// scriptedPublisher fails the messages its fail func picks and records the rest.
type scriptedPublisher struct {
	mu   sync.Mutex
	fail func(Message) error
	got  []int64
	hook func(context.Context)
}

func (p *scriptedPublisher) Publish(ctx context.Context, msgs []Message) []error {
	if p.hook != nil {
		p.hook(ctx)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		if p.fail != nil {
			errs[i] = p.fail(m)
		}
		if errs[i] == nil {
			p.got = append(p.got, m.ID)
		}
	}
	return errs
}

func msg(id int64, key string) Message {
	return Message{ID: id, Topic: "relay.events", Key: key, Payload: []byte(`{}`)}
}

func ids(fs []Failure) []int64 {
	out := make([]int64, len(fs))
	for i, f := range fs {
		out[i] = f.ID
	}
	return out
}

func TestSettleHoldsBackLaterMessagesForAFailedKey(t *testing.T) {
	boom := errors.New("broker unavailable")
	msgs := []Message{msg(1, "stripe"), msg(2, "github"), msg(3, "stripe"), msg(4, "github"), msg(5, "stripe")}
	errs := []error{nil, nil, boom, nil, nil}

	published, failed := settle(msgs, errs)

	if want := []int64{1, 2, 4}; !slices.Equal(published, want) {
		t.Fatalf("published = %v, want %v", published, want)
	}
	if want := []int64{3, 5}; !slices.Equal(ids(failed), want) {
		t.Fatalf("failed = %v, want %v", ids(failed), want)
	}
	if !errors.Is(failed[1].Err, boom) {
		t.Fatalf("held-back message should wrap the original cause, got %v", failed[1].Err)
	}
}

func TestRunOncePublishesAndMarksABatch(t *testing.T) {
	store := newMemStore(msg(1, "a"), msg(2, "b"), msg(3, "a"))
	pub := &scriptedPublisher{}
	r := &Relay{Store: store, Publisher: pub, BatchSize: 2}

	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Claimed: 2, Published: 2}) {
		t.Fatalf("result = %+v", res)
	}
	if got := store.published(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("published = %v", got)
	}
}

func TestRunOnceSettlesAnEmptyBatch(t *testing.T) {
	store := newMemStore()
	r := &Relay{Store: store, Publisher: &scriptedPublisher{}, BatchSize: 10}

	var claimed *memBatch
	r.Store = storeFunc(func(ctx context.Context, n int) (Batch, error) {
		b, err := store.Claim(ctx, n)
		claimed = b.(*memBatch)
		return b, err
	})

	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !claimed.settled {
		t.Fatal("an empty batch must still be settled to release its transaction")
	}
}

func TestRunOnceTreatsAMismatchedPublisherAsFailure(t *testing.T) {
	store := newMemStore(msg(1, "a"), msg(2, "a"))
	r := &Relay{Store: store, Publisher: publisherFunc(func(context.Context, []Message) []error { return nil }), BatchSize: 10}

	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 2 || len(store.published()) != 0 {
		t.Fatalf("nothing should be marked published, got %+v", res)
	}
}

func TestRunOnceRecordsWhatKafkaAcceptedDuringShutdown(t *testing.T) {
	store := newMemStore(msg(1, "a"))
	ctx, cancel := context.WithCancel(context.Background())
	pub := &scriptedPublisher{hook: func(context.Context) { cancel() }} // shutdown arrives mid-publish
	r := &Relay{Store: store, Publisher: pub, BatchSize: 10}

	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := store.published(); !slices.Equal(got, []int64{1}) {
		t.Fatalf("published = %v; an acknowledged message must not be republished after a clean shutdown", got)
	}
}

func TestRunWakesUpForNewMessages(t *testing.T) {
	store := newMemStore()
	pub := &scriptedPublisher{}
	wake := make(chan struct{}, 1)
	r := &Relay{Store: store, Publisher: pub, BatchSize: 10, PollInterval: time.Hour, Wake: wake}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- r.Run(ctx) }()

	store.add(msg(1, "a"))
	wake <- struct{}{}
	waitFor(t, func() bool { return len(store.published()) == 1 })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v on shutdown", err)
	}
}

func TestRunRetriesFailuresWithBackoff(t *testing.T) {
	store := newMemStore(msg(1, "a"), msg(2, "a"))
	outage := 3 // the broker rejects the first three rounds
	pub := &scriptedPublisher{}
	pub.hook = func(context.Context) {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		if outage > 0 {
			outage--
			pub.fail = func(Message) error { return errors.New("not enough replicas") }
		} else {
			pub.fail = nil
		}
	}
	r := &Relay{Store: store, Publisher: pub, BatchSize: 10, PollInterval: 50 * time.Millisecond, MaxBackoff: 40 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	waitFor(t, func() bool { return len(store.published()) == 2 })
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if !slices.Equal(pub.got, []int64{1, 2}) {
		t.Fatalf("messages reached the broker as %v; a key's messages must stay in order", pub.got)
	}
}

type storeFunc func(context.Context, int) (Batch, error)

func (f storeFunc) Claim(ctx context.Context, n int) (Batch, error) { return f(ctx, n) }

type publisherFunc func(context.Context, []Message) []error

func (f publisherFunc) Publish(ctx context.Context, msgs []Message) []error { return f(ctx, msgs) }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
