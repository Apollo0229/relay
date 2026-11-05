import { useEffect, useState } from "react";
import { addSample, type Sample } from "./lib/rates.ts";
import { duration, type Recording, windowAt } from "./lib/replay.ts";
import type { Dashboard } from "./lib/types.ts";

export type Connection = "connecting" | "live" | "reconnecting";

export interface Replay {
  recording: Recording;
  cursor: number; // seconds into the recording
  speed: number;
  setSpeed: (speed: number) => void;
  restart: () => void;
}

export interface DashboardSource {
  history: Sample[];
  latest: Dashboard | undefined;
  connection: Connection;
  now: number; // ms, on the same clock as history
  replay?: Replay;
}

/** Built with VITE_REPLAY=1, the page plays a recorded run instead of following a live API. */
export const REPLAY = import.meta.env.VITE_REPLAY === "1";

/** Follows the API's live stream and keeps the last minute of snapshots. EventSource reconnects on its own. */
export function useLiveDashboard(): DashboardSource {
  const [history, setHistory] = useState<Sample[]>([]);
  const [connection, setConnection] = useState<Connection>("connecting");
  const now = useNow();

  useEffect(() => {
    const source = new EventSource("/v1/dashboard/stream");
    source.onopen = () => setConnection("live");
    source.onerror = () => setConnection("reconnecting");
    source.onmessage = (message) => {
      const snapshot = JSON.parse(message.data) as Dashboard;
      setConnection("live");
      setHistory((h) => addSample(h, { at: Date.now(), snapshot }));
    };
    return () => source.close();
  }, []);

  return { history, latest: history.at(-1)?.snapshot, connection, now };
}

/** Plays public/replay.json on a virtual clock, looping, at 1× or faster. */
export function useReplayDashboard(): DashboardSource {
  const [recording, setRecording] = useState<Recording | null>(null);
  const [cursor, setCursor] = useState(0);
  const [speed, setSpeed] = useState(1);

  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}replay.json`)
      .then((res) => res.json() as Promise<Recording>)
      .then(setRecording)
      .catch(() => setRecording(null));
  }, []);

  useEffect(() => {
    if (!recording) return;
    const end = duration(recording);
    let last = performance.now();
    const timer = setInterval(() => {
      const t = performance.now();
      const step = ((t - last) / 1000) * speed;
      last = t;
      setCursor((c) => (c + step > end ? 0 : c + step));
    }, 250);
    return () => clearInterval(timer);
  }, [recording, speed]);

  const history = recording ? windowAt(recording, cursor) : [];
  return {
    history,
    latest: history.at(-1)?.snapshot,
    connection: recording ? "live" : "connecting",
    now: cursor * 1000,
    ...(recording && { replay: { recording, cursor, speed, setSpeed, restart: () => setCursor(0) } }),
  };
}

/** A clock that ticks four times a second, so the chart scrolls smoothly between snapshots. */
function useNow() {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(timer);
  }, []);
  return now;
}
