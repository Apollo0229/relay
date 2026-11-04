# Relay

A reliable event pipeline: webhooks in, nothing lost, every message processed once.

Relay accepts high-volume webhooks, stores each event and its outgoing message in one database transaction (the transactional outbox), and relays them to Kafka for workers to process with retries and a dead-letter queue.

## Stack

| Part | Tech |
| --- | --- |
| Ingest API | Python · FastAPI |
| Event store and outbox | PostgreSQL |
| Outbox relay | Go |
| Broker | Kafka (Redpanda locally) |
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

## Run it

```bash
docker compose up --build     # Postgres + ingest on http://localhost:8000 (API docs at /docs)

curl -X POST localhost:8000/v1/events/stripe   -H 'Content-Type: application/json' -H 'Idempotency-Key: evt_1'   -d '{"type":"payment.succeeded","data":{"amount":1200}}'
```

## Develop

```bash
cd services/ingest
uv sync
uv run pytest        # needs Postgres: docker compose up postgres
```

## License

MIT © Austin Creamer
