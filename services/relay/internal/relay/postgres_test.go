package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to RELAY_TEST_DATABASE_URL (with migrations applied) and
// empties the tables. Tests skip when it is unset.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("RELAY_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "truncate outbox, events restart identity"); err != nil {
		t.Fatal(err)
	}
	return pool
}

// seed writes one event and its outbox row per key, the way the ingest API does.
func seed(t *testing.T, pool *pgxpool.Pool, keys ...string) {
	t.Helper()
	for _, key := range keys {
		_, err := pool.Exec(context.Background(), `
			with e as (
				insert into events (id, source, type, payload)
				values (gen_random_uuid(), $1, 'test', '{}')
				returning id
			)
			insert into outbox (event_id, topic, key, payload)
			select id, 'relay.events', $1, jsonb_build_object('id', id) from e`, key)
		if err != nil {
			t.Fatal(err)
		}
	}
}

type outboxRow struct {
	ID        int64
	Published bool
	Attempts  int
	LastError *string
}

func outboxRows(t *testing.T, pool *pgxpool.Pool) []outboxRow {
	t.Helper()
	rows, _ := pool.Query(context.Background(),
		"select id, published_at is not null, attempts, last_error from outbox order by id")
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[outboxRow])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPostgresSettleRecordsEachOutcome(t *testing.T) {
	pool := testPool(t)
	seed(t, pool, "stripe", "github")
	store := &PostgresStore{Pool: pool}
	ctx := context.Background()

	batch, err := store.Claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(batch.Messages()); n != 2 {
		t.Fatalf("claimed %d messages, want 2", n)
	}
	if string(batch.Messages()[0].Payload) == "" || batch.Messages()[0].Key != "stripe" {
		t.Fatalf("unexpected message %+v", batch.Messages()[0])
	}
	if err := batch.Settle(ctx, []int64{1}, []Failure{{ID: 2, Err: errors.New("timed out")}}); err != nil {
		t.Fatal(err)
	}

	rows := outboxRows(t, pool)
	if !rows[0].Published || rows[0].Attempts != 1 || rows[0].LastError != nil {
		t.Fatalf("published row = %+v", rows[0])
	}
	if rows[1].Published || rows[1].Attempts != 1 || rows[1].LastError == nil || *rows[1].LastError != "timed out" {
		t.Fatalf("failed row = %+v", rows[1])
	}
}

func TestPostgresConcurrentClaimsNeverOverlap(t *testing.T) {
	pool := testPool(t)
	seed(t, pool, "a", "b", "c", "d", "e")
	store := &PostgresStore{Pool: pool}
	ctx := context.Background()

	first, err := store.Claim(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Claim(ctx, 3) // runs while the first claim is still open
	if err != nil {
		t.Fatal(err)
	}

	idsOf := func(b Batch) (out []int64) {
		for _, m := range b.Messages() {
			out = append(out, m.ID)
		}
		return out
	}
	if a, b := idsOf(first), idsOf(second); !slices.Equal(a, []int64{1, 2, 3}) || !slices.Equal(b, []int64{4, 5}) {
		t.Fatalf("claims = %v and %v, want disjoint batches", a, b)
	}
	for _, b := range []Batch{first, second} {
		if err := b.Settle(ctx, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgresAbandonedClaimIsRetried(t *testing.T) {
	pool := testPool(t)
	seed(t, pool, "a")
	store := &PostgresStore{Pool: pool}
	ctx := context.Background()

	// The relay dies after claiming: its transaction rolls back and the lock goes with it.
	batch, err := store.Claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.(*pgBatch).tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	again, err := store.Claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Settle(ctx, nil, nil)
	if len(again.Messages()) != 1 {
		t.Fatal("an abandoned claim must leave its messages pending")
	}
}

func TestPostgresListenWakesOnCommit(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wake := Listen(ctx, pool, slog.New(slog.NewTextHandler(io.Discard, nil)))
	<-wake // the catch-up signal sent once listening

	seed(t, pool, "a")
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("no wake-up after an outbox insert")
	}
}

func TestRelayDrainsThePostgresOutbox(t *testing.T) {
	pool := testPool(t)
	seed(t, pool, "a", "b", "a", "c", "b")
	pub := &scriptedPublisher{}
	r := &Relay{Store: &PostgresStore{Pool: pool}, Publisher: pub, BatchSize: 2}

	for range 3 {
		if _, err := r.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range outboxRows(t, pool) {
		if !row.Published {
			t.Fatalf("row %d still pending", row.ID)
		}
	}
	if !slices.Equal(pub.got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("published in order %v", pub.got)
	}
}
