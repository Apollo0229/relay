import { Activity, CircleCheckBig, History, type LucideIcon, RotateCcw, Send, Zap } from "lucide-react";
import { clock, currentMarker, duration, type Marker } from "../lib/replay.ts";
import type { Replay } from "../useDashboard.ts";

const KIND: Record<Marker["kind"], { icon: LucideIcon; tone: string }> = {
  load: { icon: Activity, tone: "text-muted" },
  events: { icon: Send, tone: "text-muted" },
  crash: { icon: Zap, tone: "text-critical" },
  recover: { icon: RotateCcw, tone: "text-good" },
  done: { icon: CircleCheckBig, tone: "text-good" },
};

const SPEEDS = [1, 4];

/** Says plainly that this is a recording, and lets the viewer speed it up or start over. */
export function ReplayBanner({ replay }: { replay: Replay }) {
  const { recording, cursor, speed, setSpeed, restart } = replay;
  const end = duration(recording);
  const recorded = new Date(recording.recorded_at).toLocaleDateString("en-US", { dateStyle: "medium" });

  return (
    <section className="rounded-xl border border-line bg-surface p-4" aria-label="Replay controls">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="flex items-start gap-2 text-sm text-ink-2">
          <History className="mt-0.5 size-4 shrink-0 text-series-1" aria-hidden />
          <span>
            <span className="font-medium text-ink">Replay of a real run</span>, recorded {recorded}.{" "}
            {recording.note} Every number is what the stack reported; this page only plays it back.
          </span>
        </p>
        <div className="flex items-center gap-1.5">
          <span className="mr-1 text-sm tabular-nums text-ink-2">
            {clock(cursor)} / {clock(end)}
          </span>
          {SPEEDS.map((s) => (
            <button
              key={s}
              type="button"
              onClick={() => setSpeed(s)}
              aria-pressed={speed === s}
              className={`rounded-md border px-2 py-0.5 text-xs ${
                speed === s ? "border-series-1 bg-series-1/10 font-medium" : "border-line hover:bg-page"
              }`}
            >
              {s}×
            </button>
          ))}
          <button
            type="button"
            onClick={restart}
            className="rounded-md border border-line px-2 py-0.5 text-xs hover:bg-page"
          >
            Restart
          </button>
        </div>
      </div>
      <div className="mt-3 h-1 rounded-full bg-grid" aria-hidden>
        <div className="h-full rounded-full bg-series-1" style={{ width: `${Math.min(100, (cursor / end) * 100)}%` }} />
      </div>
    </section>
  );
}

/** The story of the recorded run, with the current step highlighted. */
export function Timeline({ replay }: { replay: Replay }) {
  const { recording, cursor } = replay;
  const current = currentMarker(recording, cursor);

  return (
    <section className="rounded-xl border border-line bg-surface p-4">
      <h2 className="font-semibold">What happens in this run</h2>
      <p className="text-sm text-ink-2">Steady load, failing webhooks, then every service killed with SIGKILL</p>
      <ol className="mt-3 space-y-1">
        {recording.markers.map((m, i) => {
          const { icon: Icon, tone } = KIND[m.kind];
          const state = i === current ? "now" : i < current ? "past" : "next";
          return (
            <li
              key={`${m.t}-${m.label}`}
              aria-current={state === "now" ? "step" : undefined}
              className={`flex items-start gap-2 rounded-md px-2 py-1 text-sm ${
                state === "now" ? "bg-page font-medium" : state === "next" ? "opacity-45" : ""
              }`}
            >
              <span className="w-10 shrink-0 tabular-nums text-muted">{clock(m.t)}</span>
              <Icon className={`mt-0.5 size-4 shrink-0 ${tone}`} aria-hidden />
              <span>{m.label}</span>
            </li>
          );
        })}
      </ol>
      <div className="mt-4 rounded-lg bg-page p-3 text-sm">
        <p className="text-ink-2">Run it yourself, live, with buttons that send real webhooks:</p>
        <pre className="mt-2 overflow-x-auto font-mono text-xs">
          git clone https://github.com/Apollo0229/relay{"\n"}cd relay && docker compose up --build
        </pre>
      </div>
    </section>
  );
}
