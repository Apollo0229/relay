package relay

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore claims outbox rows with SELECT ... FOR UPDATE SKIP LOCKED, so
// several relays can share one outbox without publishing the same row at once.
// An empty claim is an index-only probe of outbox_pending, cheap enough to poll
// every 100ms. (A NOTIFY trigger would wake the relay sooner, but Postgres
// serialises the commits of notifying transactions, and that capped ingest.)
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
