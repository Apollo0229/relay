#!/usr/bin/env bash
# Sends one webhook through a running stack (docker compose up) and checks that
# it reaches Kafka exactly once, even when the sender retries it.
set -euo pipefail

api=${API:-http://localhost:8000}
key="e2e-$(date +%s%N)"

for _ in 1 2; do # the second send is a retry and must be deduplicated
  curl -fsS -X POST "$api/v1/events/e2e" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d '{"type":"order.created","data":{"order":42}}'
  echo
done

echo "waiting for the relay to publish..."
for _ in $(seq 60); do
  stats=$(curl -fsS "$api/v1/stats")
  if echo "$stats" | grep -q '"pending":0' && ! echo "$stats" | grep -q '"published":0'; then
    break
  fi
  sleep 1
done
echo "$stats"

echo "reading the topic..."
messages=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic relay.events --from-beginning --timeout-ms 10000 2>/dev/null || true)
echo "$messages"

count=$(echo "$messages" | grep -c '"order": 42' || true)
if [ "$count" -ne 1 ]; then
  echo "expected the event on the topic exactly once, found $count" >&2
  exit 1
fi
echo "ok: one webhook, sent twice, published once"
