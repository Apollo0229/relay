import { useEffect, useState } from "react";
import { DeadLetters } from "./components/DeadLetters.tsx";
import { ConnectionBadge, Pipeline } from "./components/Pipeline.tsx";
import { SendEvents } from "./components/SendEvents.tsx";
import { Sources } from "./components/Sources.tsx";
import { Throughput } from "./components/Throughput.tsx";
import { toRates } from "./lib/rates.ts";
import { useDashboard } from "./useDashboard.ts";

export function App() {
  const { history, latest, connection } = useDashboard();
  const now = useNow();

  return (
    <div className="mx-auto max-w-6xl px-4 py-8 sm:px-6">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Relay</h1>
          <p className="mt-1 max-w-2xl text-ink-2">
            Webhooks in, nothing lost. Each event is stored with its outbox message in one transaction, relayed to
            Kafka, and processed once, with retries and a dead-letter topic.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <ConnectionBadge state={connection} />
          <a
            href="https://github.com/Apollo0229/relay"
            className="text-sm text-ink-2 underline-offset-4 hover:underline"
            target="_blank"
            rel="noreferrer"
          >
            Source
          </a>
        </div>
      </header>

      <main className="mt-6 space-y-4">
        <Pipeline data={latest} />
        <Throughput points={toRates(history, 3)} now={now} />
        <div className="grid gap-4 lg:grid-cols-2">
          <SendEvents />
          <Sources sources={latest?.sources ?? []} />
        </div>
        <DeadLetters items={latest?.dead_letters ?? []} />
      </main>
    </div>
  );
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
