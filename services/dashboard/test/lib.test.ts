import { describe, expect, it } from "vitest";
import { demoBatch, demoEvent, sendAll, type DemoEvent } from "../src/lib/demo.ts";
import { addSample, niceMax, ticks, toRates, type Sample } from "../src/lib/rates.ts";
import type { Dashboard } from "../src/lib/types.ts";

const snap = (events: number, processed: number, dead = 0): Dashboard => ({
  events,
  pending: 0,
  published: events,
  processed,
  retried: 0,
  dead,
  sources: [],
  dead_letters: [],
});

describe("rates", () => {
  it("turns counters into per-second rates, counting dead letters as finished", () => {
    const history: Sample[] = [
      { at: 0, snapshot: snap(0, 0) },
      { at: 2000, snapshot: snap(10, 4, 2) },
    ];
    expect(toRates(history)).toEqual([{ at: 2000, received: 5, processed: 3 }]);
  });

  it("averages each rate over the last few intervals", () => {
    const history: Sample[] = [0, 10, 10, 40].map((events, i) => ({ at: i * 1000, snapshot: snap(events, 0) }));
    expect(toRates(history, 3).map((p) => p.received)).toEqual([10, 5, 40 / 3]);
  });

  it.each([
    [5, [0, 1, 2, 3, 4, 5]],
    [10, [0, 2, 4, 6, 8, 10]],
    [20, [0, 5, 10, 15, 20]],
    [50, [0, 10, 20, 30, 40, 50]],
  ])("puts round ticks on an axis up to %d", (max, expected) => {
    expect(ticks(max)).toEqual(expected);
  });

  it("reads a counter that went backwards as zero", () => {
    const history: Sample[] = [
      { at: 0, snapshot: snap(50, 50) },
      { at: 1000, snapshot: snap(0, 0) },
    ];
    expect(toRates(history)).toEqual([{ at: 1000, received: 0, processed: 0 }]);
  });

  it("keeps a sliding window of samples", () => {
    let history: Sample[] = [];
    for (let s = 0; s <= 90; s++) history = addSample(history, { at: s * 1000, snapshot: snap(s, s) });
    expect(history[0]!.at).toBe(29_000);
    expect(history.at(-1)!.at).toBe(90_000);
  });

  it.each([
    [0, 5],
    [3.2, 5],
    [7, 10],
    [12, 20],
    [43, 50],
    [180, 200],
    [999, 1000],
  ])("rounds an axis maximum of %d up to %d", (value, expected) => {
    expect(niceMax(value)).toBe(expected);
  });
});

describe("demo traffic", () => {
  it("marks simulated failures and rotates sources", () => {
    expect(demoEvent("ok", 0).data).not.toHaveProperty("simulate");
    expect(demoEvent("flaky", 0).data.simulate).toBe("flaky");
    expect(new Set(demoBatch("ok", 6).map((e) => e.source))).toEqual(new Set(["stripe", "github", "shopify"]));
    expect(new Set(demoBatch("ok", 6).map((e) => e.type)).size).toBe(6);
  });

  it("sends everything with bounded concurrency and counts failures", async () => {
    let inFlight = 0;
    let peak = 0;
    const post = async (e: DemoEvent) => {
      peak = Math.max(peak, ++inFlight);
      await new Promise((r) => setTimeout(r, 1));
      inFlight--;
      if (e.data.n === 3) throw new Error("network");
      return e.data.n !== 7;
    };
    expect(await sendAll(demoBatch("ok", 40), post, 4)).toEqual({ sent: 38, failed: 2 });
    expect(peak).toBe(4);
  });
});
