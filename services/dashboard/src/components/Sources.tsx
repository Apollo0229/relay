import type { Dashboard } from "../lib/types.ts";

const fmt = new Intl.NumberFormat("en-US");

/** Processed events per source: one hue, bars anchored at zero, values beside them. */
export function Sources({ sources }: { sources: Dashboard["sources"] }) {
  const max = Math.max(1, ...sources.map((s) => s.count));
  return (
    <section className="rounded-xl border border-line bg-surface p-4">
      <h2 className="font-semibold">Processed by source</h2>
      <p className="text-sm text-ink-2">Counted once per event, however often it was delivered</p>
      {sources.length === 0 ? (
        <p className="mt-6 text-sm text-muted">No events processed yet.</p>
      ) : (
        <ul className="mt-4 space-y-2.5">
          {sources.map((s) => (
            <li key={s.source} className="grid grid-cols-[5.5rem_1fr_auto] items-center gap-3 text-sm">
              <span className="truncate text-ink-2">{s.source}</span>
              <span className="h-3">
                <span
                  className="block h-full rounded-r bg-series-1 transition-[width] duration-500"
                  style={{ width: `${(s.count / max) * 100}%` }}
                />
              </span>
              <span className="font-medium tabular-nums">{fmt.format(s.count)}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
