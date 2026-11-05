import { describe, expect, it } from "vitest";
import { clock, currentMarker, duration, windowAt, type Recording } from "../src/lib/replay.ts";
import type { Dashboard } from "../src/lib/types.ts";

const snap = (events: number): Dashboard => ({
  events,
  pending: 0,
  published: events,
  processed: events,
  retried: 0,
  dead: 0,
  sources: [],
  dead_letters: [],
});

const rec: Recording = {
  recorded_at: "2026-11-05T19:00:00+00:00",
  note: "",
  markers: [
    { t: 3, label: "k6 starts", kind: "load" },
    { t: 30, label: "kill -9 relay", kind: "crash" },
  ],
  samples: Array.from({ length: 121 }, (_, t) => ({ t, snapshot: snap(t * 10) })),
};

describe("replay", () => {
  it("holds the last minute of samples up to the cursor, on a millisecond clock", () => {
    const w = windowAt(rec, 90.5);
    expect(w[0]!.at).toBe(30_000);
    expect(w.at(-1)).toEqual({ at: 90_000, snapshot: snap(900) });
  });

  it("starts with nothing before the first sample", () => {
    expect(windowAt(rec, -1)).toEqual([]);
  });

  it("finds the latest marker reached", () => {
    expect(currentMarker(rec, 1)).toBe(-1);
    expect(currentMarker(rec, 3)).toBe(0);
    expect(currentMarker(rec, 45)).toBe(1);
  });

  it("pauses briefly on the result before looping", () => {
    expect(duration(rec)).toBe(124);
  });

  it("formats a clock", () => {
    expect(clock(0)).toBe("0:00");
    expect(clock(83.9)).toBe("1:23");
  });
});
