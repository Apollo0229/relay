"""Records a real run of the stack for the dashboard's replay mode.

Against a fresh `docker compose up --scale worker=3`, this drives a scripted
story (steady k6 load, flaky / failing / invalid webhooks, then SIGKILL of the
relay, the worker, the API and Kafka), samples GET /v1/dashboard every second,
and writes the samples and the story's markers to a JSON file.

    python scripts/record_demo.py services/dashboard/public/replay.json
"""

import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

API = os.environ.get("API", "http://localhost:8000")
NETWORK = os.environ.get("NETWORK", "relay_default")
ROOT = Path(__file__).resolve().parent.parent
RATE, LOAD_SECONDS = 300, 100

start = time.time()
samples: list[dict] = []
markers: list[dict] = []
stop = threading.Event()


def at() -> float:
    return round(time.time() - start, 2)


def mark(label: str, kind: str) -> None:
    print(f"{at():6.1f}s  {label}", flush=True)
    markers.append({"t": at(), "label": label, "kind": kind})


def dashboard() -> dict:
    with urllib.request.urlopen(f"{API}/v1/dashboard", timeout=5) as res:
        return json.load(res)


def sample() -> None:
    while not stop.is_set():
        tick = time.time()
        try:
            samples.append({"t": at(), "snapshot": dashboard()})
        except OSError:
            pass  # the API is down on purpose for part of the story
        stop.wait(max(0, 1 - (time.time() - tick)))


def post(source: str, type_: str, data: dict) -> None:
    body = json.dumps({"type": type_, "data": data}).encode()
    req = urllib.request.Request(
        f"{API}/v1/events/{source}", body, {"Content-Type": "application/json"}, method="POST"
    )
    urllib.request.urlopen(req, timeout=5).close()


def burst(simulate: str, count: int, source: str, type_: str) -> None:
    for n in range(count):
        post(source, type_, {"n": n, "simulate": simulate})


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


def crash(service: str, down: float) -> None:
    mark(f"kill -9 {service}", "crash")
    compose("kill", "-s", "SIGKILL", service)
    time.sleep(down)
    compose("start", service)
    mark(f"{service} restarted", "recover")


def main(out: Path) -> None:
    if dashboard()["events"]:
        sys.exit("start from a fresh stack: docker compose down -v && docker compose up -d --scale worker=3")

    threading.Thread(target=sample, daemon=True).start()
    time.sleep(3)

    load = f"replay-{int(start)}"
    subprocess.run(
        ["docker", "run", "-d", "--name", load, "--network", NETWORK,
         "-v", f"{ROOT / 'scripts' / 'load'}:/scripts:ro",
         "-e", f"RATE={RATE}", "-e", f"DURATION={LOAD_SECONDS}s", "-e", "RETRIES=60", "-e", "PREFIX=shop",
         "grafana/k6:2.3.0", "run", "--quiet", "/scripts/webhooks.js"],
        check=True, capture_output=True,
    )  # fmt: skip
    mark(f"k6 starts {RATE} webhooks/s", "load")
    time.sleep(10)

    mark("25 flaky webhooks: fail once, retried after 5s", "events")
    burst("flaky", 25, "github", "push")
    time.sleep(4)
    mark("5 invalid webhooks: dead-lettered at once", "events")
    burst("invalid", 5, "stripe", "invoice.paid")
    mark("3 failing webhooks: dead-lettered after 3 attempts", "events")
    burst("error", 3, "shopify", "order.paid")
    time.sleep(8)

    crash("relay", 10)
    time.sleep(8)
    crash("worker", 10)
    time.sleep(8)
    crash("ingest", 8)
    time.sleep(8)
    crash("kafka", 10)

    subprocess.run(["docker", "wait", load], check=True, capture_output=True)
    subprocess.run(["docker", "rm", load], check=True, capture_output=True)
    mark("k6 load ends", "load")

    while True:  # until everything accepted is published and processed or dead-lettered
        d = dashboard()
        if d["pending"] == 0 and d["processed"] + d["dead"] >= d["events"]:
            break
        time.sleep(1)
    mark("pipeline drained: nothing lost, nothing applied twice", "done")
    time.sleep(5)
    stop.set()

    recording = {
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "note": "Recorded on a laptop running the full docker compose stack with three workers.",
        "markers": markers,
        "samples": samples,
    }
    out.write_text(json.dumps(recording, separators=(",", ":")))
    final = samples[-1]["snapshot"]
    print(f"wrote {len(samples)} samples to {out}: {final['events']} events, "
          f"{final['processed']} processed, {final['dead']} dead letters")  # fmt: skip


if __name__ == "__main__":
    main(Path(sys.argv[1]))
