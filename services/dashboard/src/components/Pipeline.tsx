import { ChevronRight, CircleAlert, type LucideIcon, Database, Inbox, Radio, Send, Workflow } from "lucide-react";
import type { Dashboard } from "../lib/types.ts";

interface Stage {
  label: string;
  value: number;
  icon: LucideIcon;
  note: string;
  critical?: boolean;
}

const fmt = new Intl.NumberFormat("en-US");

/** The pipeline left to right, one headline number per stage. */
export function Pipeline({ data }: { data: Dashboard | undefined }) {
  const d = data ?? { events: 0, pending: 0, published: 0, processed: 0, retried: 0, dead: 0 };
  const inWorker = Math.max(0, d.published - d.processed - d.dead);
  const stages: Stage[] = [
    { label: "Received", value: d.events, icon: Inbox, note: "stored by the API" },
    { label: "Outbox backlog", value: d.pending, icon: Database, note: "waiting for the relay" },
    { label: "Published", value: d.published, icon: Send, note: "acknowledged by Kafka" },
    {
      label: "Processed",
      value: d.processed,
      icon: Workflow,
      note: `${fmt.format(d.retried)} after a retry · ${fmt.format(inWorker)} in flight`,
    },
    { label: "Dead letters", value: d.dead, icon: CircleAlert, note: "gave up; see below", critical: d.dead > 0 },
  ];

  return (
    <ol className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-5" aria-label="Pipeline stages">
      {stages.map((s, i) => (
        <li key={s.label} className="relative rounded-xl border border-line bg-surface p-4">
          <p className="flex items-center gap-1.5 text-sm text-ink-2">
            <s.icon className={`size-4 ${s.critical ? "text-critical" : "text-muted"}`} aria-hidden />
            {s.label}
          </p>
          <p className="mt-2 text-3xl font-semibold tracking-tight">{fmt.format(s.value)}</p>
          <p className="mt-1 text-xs text-muted">
            {s.critical && <span className="font-medium text-critical">Needs attention · </span>}
            {s.note}
          </p>
          {i < stages.length - 1 && (
            <ChevronRight
              className="absolute top-1/2 -right-2.5 z-10 hidden size-4 -translate-y-1/2 rounded-full bg-page text-muted lg:block"
              aria-hidden
            />
          )}
        </li>
      ))}
    </ol>
  );
}

export function ConnectionBadge({ state }: { state: "connecting" | "live" | "reconnecting" }) {
  const live = state === "live";
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-line bg-surface px-3 py-1 text-sm">
      <Radio className={`size-4 ${live ? "text-good" : "text-warning"}`} aria-hidden />
      {live ? "Live" : state === "connecting" ? "Connecting…" : "Reconnecting…"}
    </span>
  );
}
