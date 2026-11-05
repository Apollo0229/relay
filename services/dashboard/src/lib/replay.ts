import { WINDOW_MS, type Sample } from "./rates.ts";
import type { Dashboard } from "./types.ts";

/** A real run of the stack, recorded by scripts/record_demo.py. Times are seconds from the start. */
export interface Recording {
  recorded_at: string;
  note: string;
  markers: Marker[];
  samples: { t: number; snapshot: Dashboard }[];
}

export interface Marker {
  t: number;
  label: string;
  kind: "load" | "events" | "crash" | "recover" | "done";
}

/** How long a replay runs before it starts over: the recording plus a short pause on the result. */
export function duration(rec: Recording): number {
  return (rec.samples.at(-1)?.t ?? 0) + 4;
}

/** The samples a live page would hold at `cursor` seconds into the run, on a millisecond clock. */
export function windowAt(rec: Recording, cursor: number): Sample[] {
  const from = cursor - WINDOW_MS / 1000 - 1;
  return rec.samples
    .filter((s) => s.t <= cursor && s.t >= from)
    .map((s) => ({ at: s.t * 1000, snapshot: s.snapshot }));
}

/** The latest marker reached by `cursor`, or -1 before the first. */
export function currentMarker(rec: Recording, cursor: number): number {
  return rec.markers.findLastIndex((m) => m.t <= cursor);
}

export function clock(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}
