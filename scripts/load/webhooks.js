// Sends webhooks at a fixed arrival rate, whatever the response times, so a slow
// API shows up as latency and dropped iterations rather than a quietly lower load.
import { check } from "k6";
import http from "k6/http";

const API = __ENV.API || "http://ingest:8000";
const RATE = Number(__ENV.RATE || 300);

export const options = {
  scenarios: {
    webhooks: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Math.ceil(RATE / 4),
      maxVUs: RATE * 2,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.001"],
    http_req_duration: ["p(95)<250"],
    dropped_iterations: ["count==0"],
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

// Events are keyed by source to keep each source in order, and one key lives on one
// partition, so the test spreads its traffic over many sources like real tenants.
const SOURCES = Number(__ENV.SOURCES || 32);

export default function () {
  const res = http.post(
    `${API}/v1/events/load-${(__VU + __ITER) % SOURCES}`,
    JSON.stringify({ type: "order.created", data: { vu: __VU, iteration: __ITER } }),
    { headers: { "Content-Type": "application/json", "Idempotency-Key": `${__VU}-${__ITER}-${Date.now()}` } },
  );
  check(res, { "accepted (202)": (r) => r.status === 202 });
}
