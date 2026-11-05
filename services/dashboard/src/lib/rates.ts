import type { Dashboard } from "./types.ts";

export interface Sample {
  at: number; // ms since epoch
  snapshot: Dashboard;
}

export interface RatePoint {
  at: number;
  received: number; // events accepted by the API per second
  processed: number; // events the worker finished (done or dead-lettered) per second
}

export const WINDOW_MS = 60_000;

/** Appends a sample and drops those older than the window. */
export function addSample(history: Sample[], sample: Sample, windowMs = WINDOW_MS): Sample[] {
  return [...history, sample].filter((s) => s.at >= sample.at - windowMs - 1_000);
}

/**
 * Per-second rates, each averaged over the last `span` sample intervals to smooth
 * out how requests happen to fall into one-second buckets. A counter that drops
 * (tables reset) reads as zero.
 */
export function toRates(history: Sample[], span = 1): RatePoint[] {
  const points: RatePoint[] = [];
  for (let i = 1; i < history.length; i++) {
    const prev = history[Math.max(0, i - span)]!;
    const cur = history[i]!;
    const seconds = (cur.at - prev.at) / 1000;
    if (seconds <= 0) continue;
    const finished = (s: Dashboard) => s.processed + s.dead;
    points.push({
      at: cur.at,
      received: Math.max(0, (cur.snapshot.events - prev.snapshot.events) / seconds),
      processed: Math.max(0, (finished(cur.snapshot) - finished(prev.snapshot)) / seconds),
    });
  }
  return points;
}

/** A round axis maximum (1, 2 or 5 × 10ⁿ) at or above value, and never below floor. */
export function niceMax(value: number, floor = 5): number {
  const target = Math.max(value, floor);
  const magnitude = 10 ** Math.floor(Math.log10(target));
  const step = [1, 2, 5, 10].find((m) => m * magnitude >= target)!;
  return step * magnitude;
}

/** Evenly spaced round ticks from 0 to a niceMax value: 0-50 by 10, 0-20 by 5, 0-10 by 2. */
export function ticks(max: number): number[] {
  const lead = max / 10 ** Math.floor(Math.log10(max));
  const intervals = lead === 2 ? 4 : 5;
  return Array.from({ length: intervals + 1 }, (_, i) => (max / intervals) * i);
}
