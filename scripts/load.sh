#!/usr/bin/env bash
# Load test against a running stack (docker compose up): k6 sends webhooks at a
# fixed rate, then this waits until every one has been published and processed.
#   RATE=1000 DURATION=60s scripts/load.sh
set -euo pipefail
cd "$(dirname "$0")/.."

rate=${RATE:-1000}
duration=${DURATION:-60s}
api=${API:-http://localhost:8000}
network=${NETWORK:-relay_default}
export MSYS_NO_PATHCONV=1 # keep Git Bash on Windows from rewriting container paths

field() { echo "$1" | sed -E "s/.*\"$2\":([0-9]+).*/\1/"; }
snapshot() { curl -fsS "$api/v1/dashboard"; }

before=$(snapshot)
received0=$(field "$before" events)
finished0=$(( $(field "$before" processed) + $(field "$before" dead) ))

echo "k6: $rate webhooks/s for $duration"
docker run --rm --network "$network" -v "$PWD/scripts/load:/scripts:ro" \
  -e RATE="$rate" -e DURATION="$duration" grafana/k6:2.3.0 run --quiet /scripts/webhooks.js \
  | grep -E "http_req_duration|http_reqs|http_req_failed|dropped_iterations|✓|✗" || true
load_done=$(date +%s.%N)

echo "waiting for the pipeline to drain..."
while true; do
  now=$(snapshot)
  received=$(( $(field "$now" events) - received0 ))
  finished=$(( $(field "$now" processed) + $(field "$now" dead) - finished0 ))
  [ "$finished" -ge "$received" ] && [ "$(field "$now" pending)" -eq 0 ] && break
  sleep 0.5
done
drained=$(date +%s.%N)

echo "accepted $received events; all $finished published and processed"
awk -v start="$load_done" -v end="$drained" 'BEGIN { printf "pipeline drained %.1fs after the load stopped\n", end - start }'
