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

- [ ] Ingest API with a transactional outbox
- [ ] Go relay publishing the outbox to Kafka
- [ ] Workers with retries, a dead-letter queue and idempotent processing
- [ ] Live dashboard, load test and crash test
- [ ] Hosted demo

## License

MIT © Austin Creamer
