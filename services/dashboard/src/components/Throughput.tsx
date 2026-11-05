import { useEffect, useRef, useState } from "react";
import { niceMax, type RatePoint, ticks as tickValues, WINDOW_MS } from "../lib/rates.ts";

const HEIGHT = 220;
const PAD = { top: 12, right: 96, bottom: 24, left: 32 };
const SERIES = [
  { key: "received", label: "Received", color: "var(--series-1)" },
  { key: "processed", label: "Processed", color: "var(--series-2)" },
] as const;

const rate = (n: number) => (n >= 10 ? Math.round(n).toString() : n.toFixed(1));

/** Events per second over the last minute: what the API accepts against what the worker finishes. */
export function Throughput({ points, now }: { points: RatePoint[]; now: number }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<RatePoint | null>(null);
  const [asTable, setAsTable] = useState(false);

  const plotW = Math.max(0, width - PAD.left - PAD.right);
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const yMax = niceMax(Math.max(0, ...points.flatMap((p) => [p.received, p.processed])));
  const x = (at: number) => PAD.left + ((at - (now - WINDOW_MS)) / WINDOW_MS) * plotW;
  const y = (v: number) => PAD.top + plotH - (v / yMax) * plotH;
  const ticks = tickValues(yMax);
  const visible = points.filter((p) => p.at >= now - WINDOW_MS);
  const last = visible.at(-1);

  // End labels sit beside the newest point; nudge them apart when the lines meet.
  const ends = last ? SERIES.map((s) => ({ ...s, value: last[s.key], y: y(last[s.key]) })) : [];
  if (ends.length === 2 && Math.abs(ends[0]!.y - ends[1]!.y) < 16) {
    const mid = (ends[0]!.y + ends[1]!.y) / 2;
    const upper = ends[0]!.value >= ends[1]!.value ? 0 : 1;
    ends[upper]!.y = mid - 8;
    ends[1 - upper]!.y = mid + 8;
  }

  function onMove(e: React.PointerEvent<SVGRectElement>) {
    const box = e.currentTarget.getBoundingClientRect();
    const at = now - WINDOW_MS + ((e.clientX - box.left) / box.width) * WINDOW_MS;
    let best: RatePoint | null = null;
    for (const p of visible) if (!best || Math.abs(p.at - at) < Math.abs(best.at - at)) best = p;
    setHover(best);
  }

  return (
    <section className="rounded-xl border border-line bg-surface p-4">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="font-semibold">Throughput</h2>
          <p className="text-sm text-ink-2">Events per second (3-second average), last 60 seconds</p>
        </div>
        <div className="flex items-center gap-4 text-sm text-ink-2">
          {SERIES.map((s) => (
            <span key={s.key} className="flex items-center gap-1.5">
              <span className="h-0.5 w-4 rounded-full" style={{ background: s.color }} aria-hidden />
              {s.label}
            </span>
          ))}
          <button
            type="button"
            onClick={() => setAsTable((t) => !t)}
            className="rounded-md border border-line px-2 py-0.5 text-xs hover:bg-page"
            aria-pressed={asTable}
          >
            {asTable ? "Chart" : "Table"}
          </button>
        </div>
      </header>

      <div ref={ref} className="relative mt-3">
        {asTable ? (
          <RateTable points={visible} />
        ) : (
          <>
          {width > 0 && (
            <svg width={width} height={HEIGHT} role="img" aria-label={summary(last)}>
              {ticks.map((t) => (
                <g key={t}>
                  <line
                    x1={PAD.left}
                    x2={PAD.left + plotW}
                    y1={y(t)}
                    y2={y(t)}
                    stroke={t === 0 ? "var(--axis)" : "var(--grid)"}
                  />
                  <text x={PAD.left - 6} y={y(t)} dy="0.32em" textAnchor="end" className="fill-muted text-[11px] tabular-nums">
                    {t}
                  </text>
                </g>
              ))}
              {[60, 30, 0].map((s) => (
                <text
                  key={s}
                  x={x(now - s * 1000)}
                  y={HEIGHT - 6}
                  textAnchor={s === 60 ? "start" : s === 0 ? "end" : "middle"}
                  className="fill-muted text-[11px]"
                >
                  {s === 0 ? "now" : `${s}s ago`}
                </text>
              ))}

              {SERIES.map((s) => (
                <path
                  key={s.key}
                  d={visible.map((p, i) => `${i ? "L" : "M"}${x(p.at).toFixed(1)},${y(p[s.key]).toFixed(1)}`).join("")}
                  fill="none"
                  stroke={s.color}
                  strokeWidth={2}
                  strokeLinejoin="round"
                  strokeLinecap="round"
                />
              ))}

              {last &&
                ends.map((s) => (
                  <g key={s.key}>
                    <circle cx={x(last.at)} cy={y(s.value)} r={4} fill={s.color} stroke="var(--surface)" strokeWidth={2} />
                    <text x={x(last.at) + 10} y={s.y} dy="0.32em" className="fill-ink-2 text-xs">
                      {s.label} <tspan className="fill-ink font-semibold tabular-nums">{rate(s.value)}/s</tspan>
                    </text>
                  </g>
                ))}

              {hover && (
                <g pointerEvents="none">
                  <line x1={x(hover.at)} x2={x(hover.at)} y1={PAD.top} y2={PAD.top + plotH} stroke="var(--axis)" />
                  {SERIES.map((s) => (
                    <circle
                      key={s.key}
                      cx={x(hover.at)}
                      cy={y(hover[s.key])}
                      r={4}
                      fill={s.color}
                      stroke="var(--surface)"
                      strokeWidth={2}
                    />
                  ))}
                </g>
              )}

              <rect
                x={PAD.left}
                y={PAD.top}
                width={plotW}
                height={plotH}
                fill="transparent"
                onPointerMove={onMove}
                onPointerLeave={() => setHover(null)}
              />
            </svg>
          )}

          {hover && (
            <div
              className="pointer-events-none absolute top-2 rounded-lg border border-line bg-surface px-3 py-2 text-xs shadow-lg"
              style={{
                left: Math.min(x(hover.at) + 12, width - PAD.right - 132),
              }}
            >
              <p className="mb-1 text-muted">{Math.round((now - hover.at) / 1000)}s ago</p>
              {SERIES.map((s) => (
                <p key={s.key} className="flex items-center gap-2">
                  <span className="size-2 rounded-full" style={{ background: s.color }} aria-hidden />
                  <span className="text-ink-2">{s.label}</span>
                  <span className="ml-auto pl-3 font-semibold tabular-nums">{rate(hover[s.key])}/s</span>
                </p>
              ))}
            </div>
          )}

          {visible.length < 2 && (
            <p className="absolute inset-0 grid place-items-center text-sm text-muted">Waiting for data…</p>
          )}
          </>
        )}
      </div>
    </section>
  );
}

function RateTable({ points }: { points: RatePoint[] }) {
  const rows = points.slice(-15).reverse();
  return (
    <table className="w-full text-sm">
      <thead className="text-left text-muted">
        <tr>
          <th className="py-1 font-normal">Time</th>
          <th className="py-1 text-right font-normal">Received/s</th>
          <th className="py-1 text-right font-normal">Processed/s</th>
        </tr>
      </thead>
      <tbody className="tabular-nums">
        {rows.map((p) => (
          <tr key={p.at} className="border-t border-line">
            <td className="py-1">{new Date(p.at).toLocaleTimeString()}</td>
            <td className="py-1 text-right">{rate(p.received)}</td>
            <td className="py-1 text-right">{rate(p.processed)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function summary(last: RatePoint | undefined) {
  if (!last) return "Throughput chart, waiting for data";
  return `Throughput: ${rate(last.received)} received and ${rate(last.processed)} processed per second`;
}

function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry!.contentRect.width));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  return [ref, width] as const;
}
