#!/usr/bin/env bash
# Sends one webhook through a running stack (docker compose up) and checks that
# it reaches Kafka exactly once, even when the sender retries it.
set -euo pipefail

api=${API:-http://localhost:8000}
key="e2e-$(date +%s%N)"

send() {
  curl -fsS -X POST "$api/v1/events/e2e" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d '{"type":"order.created","data":{"order":42}}'
}
first=$(send)
retry=$(send) # same key: must return the original event, not store a new one
echo "$first"
echo "$retry"
id=$(echo "$first" | sed -E 's/.*"id":"([^"]+)".*/\1/')

echo "waiting for the relay to publish..."
for _ in $(seq 60); do
  stats=$(curl -fsS "$api/v1/stats")
  if echo "$stats" | grep -q '"pending":0'; then
    break
  fi
  sleep 1
done
echo "$stats"

echo "reading the topic..."
# MSYS_NO_PATHCONV stops Git Bash on Windows from rewriting the container path.
messages=$(MSYS_NO_PATHCONV=1 docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic relay.events --from-beginning --timeout-ms 10000 2>/dev/null || true)

# Match on the event id, so the check also holds on a stack that already has traffic.
count=$(echo "$messages" | grep -c "\"id\": \"$id\"" || true)
if [ "$count" -ne 1 ]; then
  echo "expected event $id on the topic exactly once, found $count" >&2
  exit 1
fi
echo "ok: event $id was sent twice and published once"
