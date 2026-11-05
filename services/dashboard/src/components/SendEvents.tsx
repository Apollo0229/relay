import { Send } from "lucide-react";
import { useState } from "react";
import { demoBatch, type Kind, sendAll } from "../lib/demo.ts";

const KINDS: { kind: Kind; label: string; hint: string }[] = [
  { kind: "ok", label: "Normal", hint: "Processed on the first attempt." },
  { kind: "flaky", label: "Flaky", hint: "Fails once, then succeeds on the 5s retry." },
  { kind: "error", label: "Failing", hint: "Fails every attempt; dead-lettered after about 35s." },
  { kind: "invalid", label: "Invalid", hint: "Dead-lettered at once, without retries." },
];
const COUNTS = [1, 25, 250];

/** Sends demo webhooks through the real API, so every number on the page moves. */
export function SendEvents() {
  const [kind, setKind] = useState<Kind>("ok");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  async function send(count: number) {
    setBusy(true);
    setResult(null);
    const started = performance.now();
    const { sent, failed } = await sendAll(demoBatch(kind, count));
    const seconds = (performance.now() - started) / 1000;
    setResult(`Sent ${sent} in ${seconds.toFixed(1)}s${failed ? `, ${failed} rejected` : ""}`);
    setBusy(false);
  }

  const active = KINDS.find((k) => k.kind === kind)!;
  return (
    <section className="rounded-xl border border-line bg-surface p-4">
      <h2 className="font-semibold">Send webhooks</h2>
      <p className="text-sm text-ink-2">Real requests to POST /v1/events, each with its own idempotency key</p>

      <div role="radiogroup" aria-label="Event kind" className="mt-4 grid grid-cols-2 gap-1.5 sm:grid-cols-4">
        {KINDS.map((k) => (
          <button
            key={k.kind}
            type="button"
            role="radio"
            aria-checked={kind === k.kind}
            onClick={() => setKind(k.kind)}
            className={`rounded-lg border px-3 py-1.5 text-sm transition ${
              kind === k.kind ? "border-series-1 bg-series-1/10 font-medium" : "border-line hover:bg-page"
            }`}
          >
            {k.label}
          </button>
        ))}
      </div>
      <p className="mt-2 text-xs text-muted">{active.hint}</p>

      <div className="mt-4 flex flex-wrap items-center gap-2">
        {COUNTS.map((n) => (
          <button
            key={n}
            type="button"
            disabled={busy}
            onClick={() => send(n)}
            className="inline-flex items-center gap-1.5 rounded-lg bg-ink px-3 py-1.5 text-sm font-medium text-page transition hover:opacity-85 disabled:opacity-50"
          >
            <Send className="size-3.5" aria-hidden />
            Send {n}
          </button>
        ))}
        <span className="text-sm text-ink-2" aria-live="polite">
          {busy ? "Sending…" : result}
        </span>
      </div>
    </section>
  );
}
