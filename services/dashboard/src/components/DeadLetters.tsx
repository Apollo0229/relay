import { CircleAlert } from "lucide-react";
import type { DeadLetter } from "../lib/types.ts";

/** The most recent events the worker gave up on, with the reason. */
export function DeadLetters({ items }: { items: DeadLetter[] }) {
  return (
    <section className="rounded-xl border border-line bg-surface p-4">
      <h2 className="font-semibold">Recent dead letters</h2>
      <p className="text-sm text-ink-2">Sent to relay.events.dlq after their last retry, or at once if invalid</p>
      {items.length === 0 ? (
        <p className="mt-6 text-sm text-muted">None. Send a “Failing” or “Invalid” event to see one.</p>
      ) : (
        <div className="mt-3">
          <table className="w-full text-sm">
            <thead className="text-left text-muted">
              <tr>
                <th className="py-1.5 font-normal">Event</th>
                <th className="hidden py-1.5 font-normal sm:table-cell">Attempts</th>
                <th className="py-1.5 font-normal">Error</th>
                <th className="hidden py-1.5 text-right font-normal sm:table-cell">When</th>
              </tr>
            </thead>
            <tbody>
              {items.map((d) => (
                <tr key={d.id} className="border-t border-line align-top">
                  <td className="py-2 pr-3">
                    <p className="font-medium">
                      {d.source} · {d.type}
                    </p>
                    <p className="text-xs text-muted">
                      <span className="font-mono">{d.id.slice(0, 8)}</span>
                      <span className="sm:hidden">
                        {" "}
                        · {d.attempts} {d.attempts === 1 ? "attempt" : "attempts"} · {new Date(d.at).toLocaleTimeString()}
                      </span>
                    </p>
                  </td>
                  <td className="hidden py-2 pr-3 tabular-nums sm:table-cell">{d.attempts}</td>
                  <td className="py-2 pr-3">
                    <span className="inline-flex items-start gap-1.5 text-ink-2">
                      <CircleAlert className="mt-0.5 size-4 shrink-0 text-critical" aria-label="Failed" />
                      {d.error ?? "unknown"}
                    </span>
                  </td>
                  <td className="hidden py-2 text-right whitespace-nowrap text-muted sm:table-cell">
                    {new Date(d.at).toLocaleTimeString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
