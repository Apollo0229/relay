package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore claims outbox rows with SELECT ... FOR UPDATE SKIP LOCKED, so
// several relays can share one outbox without publishing the same row at once.
type PostgresStore struct {
	Pool *pgxpool.Pool
}

func (s *PostgresStore) Claim(ctx context.Context, limit int) (Batch, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	rows, _ := tx.Query(ctx, `
		select id, topic, key, payload::text from outbox
		where published_at is null
		order by id
		limit $1
		for update skip locked`, limit)
	msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return &pgBatch{tx: tx, msgs: msgs}, nil
}

type pgBatch struct {
	tx   pgx.Tx
	msgs []Message
}

func (b *pgBatch) Messages() []Message { return b.msgs }

func (b *pgBatch) Settle(ctx context.Context, published []int64, failed []Failure) error {
	defer b.tx.Rollback(ctx) // no-op once committed

	if len(published) > 0 {
		if _, err := b.tx.Exec(ctx, `
			update outbox
			set published_at = now(), attempts = attempts + 1, last_error = null
			where id = any($1)`, published); err != nil {
			return err
		}
	}
	if len(failed) > 0 {
		ids := make([]int64, len(failed))
		reasons := make([]string, len(failed))
		for i, f := range failed {
			ids[i], reasons[i] = f.ID, f.Err.Error()
		}
		if _, err := b.tx.Exec(ctx, `
			update outbox o
			set attempts = o.attempts + 1, last_error = f.reason
			from unnest($1::bigint[], $2::text[]) as f(id, reason)
			where o.id = f.id`, ids, reasons); err != nil {
			return err
		}
	}
	return b.tx.Commit(ctx)
}

// Listen delivers a signal whenever the outbox_notify trigger fires. It holds
// one pooled connection and reconnects after errors until ctx ends. Signals
// coalesce: a burst of inserts wakes the relay once.
func Listen(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) <-chan struct{} {
	wake := make(chan struct{}, 1)
	signal := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	go func() {
		for ctx.Err() == nil {
			err := listen(ctx, pool, signal)
			if ctx.Err() != nil {
				return
			}
			log.Warn("outbox listener lost its connection; polling until it reconnects", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()
	return wake
}

func listen(ctx context.Context, pool *pgxpool.Pool, signal func()) error {
	pooled, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// A LISTENing connection must not go back to the pool.
	conn := pooled.Hijack()
	defer conn.Close(context.WithoutCancel(ctx))

	if _, err := conn.Exec(ctx, "listen outbox"); err != nil {
		return err
	}
	signal() // catch up on anything written while we were not listening
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		signal()
	}
}
