# Relay

A reliable event pipeline: webhooks in, nothing lost, every message processed once.

Relay accepts high-volume webhooks, stores each event and its outgoing message in one database transaction (the transactional outbox), and relays them to Kafka for workers to process with retries and a dead-letter queue.

## Stack

| Part | Tech |
| --- | --- |
| Ingest API | Python · FastAPI |
| Event store and outbox | PostgreSQL |
| Outbox relay | Go |
| Broker | Kafka (KRaft) |
| Workers | Python |
| Dashboard | React · TypeScript |

## Status

- [x] Ingest API with a transactional outbox
- [ ] Go relay publishing the outbox to Kafka
- [ ] Workers with retries, a dead-letter queue and idempotent processing
- [ ] Live dashboard, load test and crash test
- [ ] Hosted demo

## Ingest API

```http
POST /v1/events/stripe
Idempotency-Key: evt_1PqX…
Content-Type: application/json

{ "type": "payment.succeeded", "data": { "amount": 1200, "currency": "usd" } }
```

- **Atomic:** the event and its outbox message commit in one transaction, so neither exists without the other.
- **Idempotent:** a repeated `Idempotency-Key` for the same source returns the original event (`200`, `"duplicate": true`) instead of storing it again, even when retries arrive at the same moment.
- **Ordered per source:** outbox messages are keyed by source, so one source's events stay on one Kafka partition.

`GET /v1/stats` reports stored, pending and published counts; `GET /healthz` checks the database.

## Outbox relay

A Go service that moves the outbox to Kafka.

- **Wakes on commit:** a trigger sends `NOTIFY outbox` when rows are written, so messages go out immediately; a 1s poll covers a dropped listener.
- **Safe to scale out:** batches are claimed with `FOR UPDATE SKIP LOCKED`, so relays sharing one outbox never publish the same row at once. One relay keeps strict per-source order; more add throughput and failover.
- **At least once:** a row is marked published in the same transaction that claimed it, only after Kafka acknowledges it (all in-sync replicas, idempotent producer). A crash in between republishes the batch, and consumers drop the copy by event id.
- **Ordered retries:** when a message fails, later messages for the same key in that batch are held back too, so a retry replays the key's messages in order. Failed rounds back off exponentially, and each row records `attempts` and `last_error`.

| Setting | Default |
| --- | --- |
| `RELAY_DATABASE_URL` | required |
| `RELAY_KAFKA_BROKERS` | required, comma-separated |
| `RELAY_BATCH_SIZE` | `500` |
| `RELAY_POLL_INTERVAL` | `1s` |
| `RELAY_MAX_BACKOFF` | `30s` |

## Run it

```bash
docker compose up --build     # Postgres, Kafka, relay, and ingest on http://localhost:8000 (API docs at /docs)

curl -X POST localhost:8000/v1/events/stripe   -H 'Content-Type: application/json' -H 'Idempotency-Key: evt_1'   -d '{"type":"payment.succeeded","data":{"amount":1200}}'

scripts/e2e.sh                # one webhook, sent twice, must reach Kafka once
```

## Develop

```bash
cd services/ingest
uv sync
uv run pytest        # needs Postgres: docker compose up postgres

cd services/relay
RELAY_TEST_DATABASE_URL=postgres://relay:relay@localhost:5432/relay RELAY_TEST_KAFKA_BROKERS=localhost:9092 go test ./...   # integration tests skip when unset
```

## License

MIT © Austin Creamer
