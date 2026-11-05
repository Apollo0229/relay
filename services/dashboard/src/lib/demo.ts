/** Demo traffic for the dashboard's "send events" panel. */

export type Kind = "ok" | "flaky" | "error" | "invalid";

export interface DemoEvent {
  source: string;
  type: string;
  data: Record<string, unknown>;
}

const SOURCES = [
  { source: "stripe", types: ["payment.succeeded", "invoice.paid"] },
  { source: "github", types: ["push", "pull_request.opened"] },
  { source: "shopify", types: ["order.created", "order.paid"] },
] as const;

/** The nth event of a batch: sources and types rotate so the charts show a mix. */
export function demoEvent(kind: Kind, n: number): DemoEvent {
  const { source, types } = SOURCES[n % SOURCES.length]!;
  const type = types[Math.floor(n / SOURCES.length) % types.length]!;
  const data: Record<string, unknown> = { n, amount: 100 + ((n * 37) % 900) };
  if (kind !== "ok") data.simulate = kind;
  return { source, type, data };
}

export function demoBatch(kind: Kind, count: number): DemoEvent[] {
  return Array.from({ length: count }, (_, n) => demoEvent(kind, n));
}

export type Post = (event: DemoEvent) => Promise<boolean>;

export const postEvent: Post = async ({ source, type, data }) => {
  const res = await fetch(`/v1/events/${source}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": crypto.randomUUID() },
    body: JSON.stringify({ type, data }),
  });
  return res.ok;
};

/** Sends events with at most `concurrency` requests in flight. */
export async function sendAll(events: DemoEvent[], post: Post = postEvent, concurrency = 16) {
  let next = 0;
  let sent = 0;
  let failed = 0;
  async function lane() {
    while (next < events.length) {
      const event = events[next++]!;
      const ok = await post(event).catch(() => false);
      if (ok) sent++;
      else failed++;
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, events.length) }, lane));
  return { sent, failed };
}
