# Relay

A reliable event pipeline: webhooks in, nothing lost, every message processed once.

Relay accepts high-volume webhooks, stores each event and its outgoing message in one database transaction (the transactional outbox), and relays them to Kafka for workers to process with retries and a dead-letter queue.

**[Watch the crash test →](https://apollo0229.github.io/relay/)** The dashboard replays a real run: steady load, failing webhooks, then the relay, worker, API and Kafka each killed with `SIGKILL`. Free hosts can't run Kafka, so the public page plays back a recording; `docker compose up` runs the same dashboard live.

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
- [x] Go relay publishing the outbox to Kafka
- [x] Workers with retries, a dead-letter queue and idempotent processing
- [x] Live dashboard, load test and crash test
- [x] Hosted demo (a replay on GitHub Pages)

## Results

Measured on one laptop running the whole stack in Docker Desktop, with k6 on the same machine.

**Load** (`scripts/load.sh`, 60s at a fixed arrival rate, 32 sources):

| Rate | API p95 | Failed | Workers | Drained after the load stopped |
| --- | --- | --- | --- | --- |
| 1,000/s | 55ms | 0 of 60,001 | 3 | 28.7s |
| 1,000/s | 54ms | 0 of 59,943 | 6 | 4.1s |
| 1,500/s | 143ms | 0 of 89,983 | 3 | 67.4s |

Every accepted event was published and processed in each run. With six workers, one per partition, processing keeps pace with 1,000 events/s.

**Crashes** (`scripts/crash.sh`): while k6 sent 300 webhooks/s, the relay, the worker, the API and Kafka were each killed with `SIGKILL` and restarted. Senders retried failed requests with the same idempotency key.

| | |
| --- | --- |
| Requests that failed during the crashes | 1,996 |
| Webhooks acknowledged / stored | 21,295 / 21,295 |
| Events processed / effects applied | 21,295 / 21,295 |
| Redeliveries the worker skipped | 198 |

Nothing lost, nothing applied twice.

**What the measurements changed.** The first version woke the relay with a `NOTIFY` trigger on every outbox insert. Under load, ingest stalled at ~340 events/s with multi-second latency: Postgres serialises the commits of all transactions that send `NOTIFY` on one lock. Polling an index of pending rows every 100ms removed the ceiling. Committing Kafka offsets in the background once a second, instead of after every message, took a worker from ~100 to ~350 events/s.

## Ingest API

```http
POST /v1/events/stripe
Idempotency-Key: evt_1PqX…
Content-Type: application/json

{ "type": "payment.succeeded", "data": { "amount": 1200, "currency": "usd" } }
```

- **Atomic:** the event and its outbox message commit in one transaction, so neither exists without the other.
- **Idempotent:** a repeated `Idempotency-Key` for the same source returns the original event (`200`, `"duplicate": true`) instead of storing it again, even when retries arrive at the same moment.
- **Ordered per source:** outbox messages are keyed by source, so one source's events stay on one Kafka partition. The flip side: a single very busy source is limited to one consumer.

`GET /v1/dashboard` reports the whole pipeline, and `GET /v1/dashboard/stream` sends it every second as server-sent events. `GET /v1/stats` gives the short version; `GET /healthz` checks the database.

## Outbox relay

A Go service that moves the outbox to Kafka.

- **Polls cheaply:** while the outbox is empty, each 100ms poll is an index-only probe of the pending rows; while it has rows, the relay claims batch after batch without waiting.
- **Safe to scale out:** batches are claimed with `FOR UPDATE SKIP LOCKED`, so relays sharing one outbox never publish the same row at once. One relay keeps strict per-source order; more add throughput and failover.
- **At least once:** a row is marked published in the same transaction that claimed it, only after Kafka acknowledges it (all in-sync replicas, idempotent producer). A crash in between republishes the batch, and consumers drop the copy by event id.
- **Ordered retries:** when a message fails, later messages for the same key in that batch are held back too, so a retry replays the key's messages in order. Failed rounds back off exponentially, and each row records `attempts` and `last_error`.

| Setting | Default |
| --- | --- |
| `RELAY_DATABASE_URL` | required |
| `RELAY_KAFKA_BROKERS` | required, comma-separated |
| `RELAY_BATCH_SIZE` | `500` |
| `RELAY_POLL_INTERVAL` | `100ms` |
| `RELAY_MAX_BACKOFF` | `30s` |

## Workers

Python consumers that apply each event once, however often Kafka delivers it.

- **Exactly-once effects:** a worker records `(consumer, event_id)` in `processed_events` in the same transaction as the event's writes. A redelivered message finds the row and is skipped; two workers given the same event at once serialise on its primary key.
- **Retries without blocking:** a failed event moves to `relay.events.retry.1` (5s), then `relay.events.retry.2` (30s), each served by its own consumer, so a slow retry never holds up fresh events. Headers carry the attempt number, the last error and when it may run.
- **Dead-letter topic:** after the last retry, or at once for a permanent error such as a malformed payload, the event goes to `relay.events.dlq` and is recorded with status `dead`.
- **Outages are not the event's fault:** if Postgres or Kafka is down, the worker puts the message back and backs off rather than spending a retry.
- **Cheap commits:** an offset is stored once its message is handled or forwarded and committed in the background each second. A crash replays at most that second, and the replays are skipped.

Retried events can overtake newer ones from the same source, the usual trade for non-blocking retries; per-source order holds while nothing fails.

The demo handler counts events per source and type in `event_counts`. Put `"simulate": "flaky"`, `"error"` or `"invalid"` in an event's `data` to watch a retry, a dead letter after three attempts, or an immediate dead letter.

| Setting | Default |
| --- | --- |
| `WORKER_DATABASE_URL` | `postgresql://relay:relay@localhost:5432/relay` |
| `WORKER_KAFKA_BROKERS` | `localhost:9092` |
| `WORKER_RETRY_DELAYS` | `[5, 30]` (seconds, one per retry) |
| `WORKER_GROUP_ID` | `relay-workers` |

## Dashboard

A React page on the live stream: a headline number for each stage (received, outbox backlog, published, processed, dead letters), received against processed events per second, events per source, recent dead letters with their errors, and buttons that send normal, flaky, failing or invalid webhooks through the real API. nginx serves it and proxies `/v1` to the API.

Built with `VITE_REPLAY=1` it plays `public/replay.json` instead, a run recorded by `scripts/record_demo.py`; that is the version on GitHub Pages.

## Run it

```bash
docker compose up --build --scale worker=3
# dashboard  http://localhost:8080
# API        http://localhost:8000/docs

curl -X POST localhost:8000/v1/events/stripe \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: evt_1' \
  -d '{"type":"payment.succeeded","data":{"amount":1200}}'

scripts/e2e.sh                        # a retried webhook is published and processed once; an invalid one is dead-lettered
RATE=1000 DURATION=60s scripts/load.sh  # k6 load test, then the time the pipeline takes to drain
scripts/crash.sh                      # kill every service mid-load, then check nothing was lost or applied twice
```

## Develop

The tests empty the tables they use, so point them at a separate database while the stack runs:

```bash
docker compose exec postgres psql -U relay -c 'create database relay_test'
for f in db/migrations/*.sql; do docker compose exec -T postgres psql -U relay -d relay_test < "$f"; done
db=postgresql://relay:relay@localhost:5432/relay_test

(cd services/ingest && RELAY_DATABASE_URL=$db uv run pytest)
(cd services/relay && RELAY_TEST_DATABASE_URL=$db RELAY_TEST_KAFKA_BROKERS=localhost:9092 go test ./...)
(cd services/worker && WORKER_DATABASE_URL=$db WORKER_TEST_KAFKA_BROKERS=localhost:9092 uv run pytest)
(cd services/dashboard && npm ci && npm test)
```

Integration tests that need Kafka skip when its variable is unset.

## License

MIT © Austin Creamer
