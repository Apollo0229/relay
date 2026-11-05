#!/usr/bin/env bash
# Crash test against a running stack (docker compose up). While k6 sends steady
# traffic, the relay, the worker, the API and Kafka are each killed with SIGKILL
# and restarted. Afterwards every acknowledged webhook must be stored once,
# published, and processed exactly once.
#   RATE=300 scripts/crash.sh
set -euo pipefail
cd "$(dirname "$0")/.."

rate=${RATE:-300}
network=${NETWORK:-relay_default}
run="crash-$(date +%s)"
export MSYS_NO_PATHCONV=1 # keep Git Bash on Windows from rewriting container paths

sql() { docker compose exec -T postgres psql -U relay -d relay -tAc "$1"; }
topic_size() { # messages ever written to relay.events, summed over partitions
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic relay.events | awk -F: '{ n += $3 } END { print n }'
}
at() { printf '%5.1fs  %s\n' "$(awk -v s="$start" -v n="$(date +%s.%N)" 'BEGIN { print n - s }')" "$*"; }
crash() { # crash <service> <seconds down>
  at "kill -9 $1"
  docker compose kill -s SIGKILL "$1" > /dev/null 2>&1
  sleep "$2"
  at "restart $1"
  docker compose start "$1" > /dev/null 2>&1
}

written_before=$(topic_size)
echo "k6: $rate webhooks/s for 75s from sources $run-*, retrying failed sends with the same key"
docker run -d --name "$run" --network "$network" -v "$PWD/scripts/load:/scripts:ro" \
  -e RATE="$rate" -e DURATION=75s -e PREFIX="$run" -e RETRIES=60 \
  grafana/k6:2.3.0 run --quiet /scripts/webhooks.js > /dev/null
start=$(date +%s.%N)
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)

sleep 8;  crash relay 8
sleep 6;  crash worker 8
sleep 6;  crash ingest 6
sleep 6;  crash kafka 10
docker wait "$run" > /dev/null
at "load finished"
summary=$(docker logs "$run" 2>&1)
docker rm "$run" > /dev/null
delivered=$(echo "$summary" | sed -nE 's/.*webhooks_delivered[^0-9]*([0-9]+).*/\1/p')
undelivered=$(echo "$summary" | sed -nE 's/.*webhooks_undelivered[^0-9]*([0-9]+).*/\1/p')
failed_requests=$(echo "$summary" | sed -nE 's/.*http_req_failed[^0-9]*[0-9.]+% *([0-9]+) out of.*/\1/p')

ours="from events where source like '$run-%'"
echo "waiting for the pipeline to drain..."
for _ in $(seq 600); do
  stored=$(sql "select count(*) $ours")
  processed=$(sql "select count(*) from processed_events p join events e on e.id = p.event_id
                   where e.source like '$run-%' and p.status = 'done'")
  pending=$(sql "select count(*) from outbox where published_at is null")
  [ "$processed" -ge "$stored" ] && [ "$pending" -eq 0 ] && break
  sleep 0.5
done
at "drained"

counted=$(sql "select coalesce(sum(count), 0) from event_counts where source like '$run-%'")
dead=$(sql "select count(*) from processed_events p join events e on e.id = p.event_id
            where e.source like '$run-%' and p.status = 'dead'")
republished=$(( $(topic_size) - written_before - stored ))
skipped=$(docker compose logs worker --since "$since" 2>/dev/null | grep -c "skipped duplicate" || true)

echo
echo "requests that failed during the crashes:  ${failed_requests:-0} (retried by the sender)"
echo "webhooks acknowledged by the API:         ${delivered:-0}"
echo "webhooks never acknowledged:              ${undelivered:-0}"
echo "events stored:                            $stored"
echo "events processed:                         $processed (dead-lettered: $dead)"
echo "effects applied (event_counts total):     $counted"
echo "messages the relay published twice:       $republished"
echo "redeliveries the worker skipped:          $skipped"

ok=true
[ "${undelivered:-0}" -eq 0 ] || { echo "FAIL: some webhooks were never acknowledged" >&2; ok=false; }
[ "${delivered:-0}" -eq "$stored" ] || { echo "FAIL: acknowledged $delivered webhooks but stored $stored" >&2; ok=false; }
[ "$processed" -eq "$stored" ] || { echo "FAIL: stored $stored events but processed $processed" >&2; ok=false; }
[ "$counted" -eq "$stored" ] || { echo "FAIL: $stored events applied $counted times" >&2; ok=false; }
[ "$dead" -eq 0 ] || { echo "FAIL: $dead events were dead-lettered" >&2; ok=false; }
$ok && echo "ok: nothing lost, nothing processed twice"
$ok
