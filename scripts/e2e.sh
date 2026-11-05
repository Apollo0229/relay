#!/usr/bin/env bash
# Follows webhooks through a running stack (docker compose up): a retried
# webhook must reach Kafka once and be processed once, and an invalid one must
# end up in the dead-letter topic.
set -euo pipefail

api=${API:-http://localhost:8000}
run=$(date +%s%N)
# MSYS_NO_PATHCONV stops Git Bash on Windows from rewriting container paths.
export MSYS_NO_PATHCONV=1

send() { # send <idempotency key> <json body>
  curl -fsS -X POST "$api/v1/events/e2e" -H 'Content-Type: application/json' -H "Idempotency-Key: $1" -d "$2"
}
id_of() { sed -E 's/.*"id":"([^"]+)".*/\1/'; }
sql() { docker compose exec -T postgres psql -U relay -d relay -tAc "$1"; }
topic() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --from-beginning --timeout-ms 10000 2>/dev/null || true
}
fail() { echo "FAIL: $*" >&2; exit 1; }

order='{"type":"order.created","data":{"order":42}}'
first=$(send "order-$run" "$order")
retry=$(send "order-$run" "$order") # same key: must return the original event
echo "$first"
echo "$retry"
id=$(echo "$first" | id_of)
[ "$(echo "$retry" | id_of)" = "$id" ] || fail "the retried webhook was stored as a new event"

bad=$(send "bad-$run" '{"type":"order.created","data":{"simulate":"invalid"}}' | id_of)

echo "waiting for the worker..."
for _ in $(seq 60); do
  done_status=$(sql "select status from processed_events where event_id = '$id'")
  bad_status=$(sql "select status from processed_events where event_id = '$bad'")
  [ "$done_status" = done ] && [ "$bad_status" = dead ] && break
  sleep 1
done
curl -fsS "$api/v1/stats"; echo
[ "$done_status" = done ] || fail "event $id was not processed (status: '${done_status}')"
[ "$bad_status" = dead ] || fail "invalid event $bad was not dead-lettered (status: '${bad_status}')"

published=$(topic relay.events | grep -c "\"id\": \"$id\"" || true)
[ "$published" -eq 1 ] || fail "expected event $id on relay.events once, found $published"
dead=$(topic relay.events.dlq | grep -c "\"id\": \"$bad\"" || true)
[ "$dead" -eq 1 ] || fail "expected event $bad on relay.events.dlq once, found $dead"

echo "ok: $id sent twice, published once, processed once; $bad dead-lettered"
