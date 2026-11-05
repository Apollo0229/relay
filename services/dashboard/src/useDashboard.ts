import { useEffect, useState } from "react";
import { addSample, type Sample } from "./lib/rates.ts";
import type { Dashboard } from "./lib/types.ts";

export type Connection = "connecting" | "live" | "reconnecting";

/** Follows the API's live stream and keeps the last minute of snapshots. EventSource reconnects on its own. */
export function useDashboard() {
  const [history, setHistory] = useState<Sample[]>([]);
  const [connection, setConnection] = useState<Connection>("connecting");

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

  return { history, latest: history.at(-1)?.snapshot, connection };
}
