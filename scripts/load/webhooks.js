// Sends webhooks at a fixed arrival rate, whatever the response times, so a slow
// API shows up as latency and dropped iterations rather than a quietly lower load.
//
// With RETRIES > 0 a failed send is retried with the same idempotency key, as
// webhook providers do; the crash test relies on that while it kills the API.
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";
import http from "k6/http";

const API = __ENV.API || "http://ingest:8000";
const RATE = Number(__ENV.RATE || 300);
const RETRIES = Number(__ENV.RETRIES || 0);
const PREFIX = __ENV.PREFIX || "load";

export const options = {
  scenarios: {
    webhooks: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Math.ceil(RATE / 4),
      maxVUs: RATE * (RETRIES ? 30 : 2),
    },
  },
  // Under deliberate crashes some requests fail by design; the crash test checks the data instead.
  thresholds: RETRIES
    ? {}
    : {
        http_req_failed: ["rate<0.001"],
        http_req_duration: ["p(95)<250"],
        dropped_iterations: ["count==0"],
      },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

const delivered = new Counter("webhooks_delivered");
const undelivered = new Counter("webhooks_undelivered");

// Events are keyed by source to keep each source in order, and one key lives on one
// partition, so the test spreads its traffic over many sources like real tenants.
const SOURCES = Number(__ENV.SOURCES || 32);

export default function () {
  const url = `${API}/v1/events/${PREFIX}-${(__VU + __ITER) % SOURCES}`;
  const body = JSON.stringify({ type: "order.created", data: { vu: __VU, iteration: __ITER } });
  const params = {
    headers: { "Content-Type": "application/json", "Idempotency-Key": `${__VU}-${__ITER}-${Date.now()}` },
    timeout: "5s",
  };

  let res = http.post(url, body, params);
  for (let attempt = 0; attempt < RETRIES && !(res.status === 200 || res.status === 202); attempt++) {
    sleep(0.5);
    res = http.post(url, body, params);
  }
  const ok = check(res, { "accepted (200 or 202)": (r) => r.status === 200 || r.status === 202 });
  (ok ? delivered : undelivered).add(1);
}
