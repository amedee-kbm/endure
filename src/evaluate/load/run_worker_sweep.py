"""
RQ1 worker-sweep script — runs Locust headlessly for each worker count 1..N,
collects throughput and latency, writes sweep_summary.json.

Usage:
  uv run python src/evaluate/load/run_worker_sweep.py
  # or override defaults:
  MAX_WORKERS=6 RUNS=3 JOBS_PER_RUN=50 uv run python ...

Environment variables:
  MAX_WORKERS       highest worker count to test (default 8)
  RUNS              Locust runs per worker count for median (default 3)
  RUN_TIME          Locust --run-time per run (default 60s)
  LOCUST_USERS      -u (concurrent users) passed to Locust (default 10)
  LOCUST_SPAWN_RATE -r passed to Locust (default 2)
  STAGE_DURATION    seconds per job stage (default 0.05)
  RESULTS_DIR       output directory (default loadtest-results/worker-sweep)
  API_URL           endure API URL for health-check (default http://localhost:8000)

Requires:
  - docker compose (v2) on PATH
  - locust on PATH (uv run locust or pip install locust)
  - The project Dockerfile built: docker compose build
"""

from __future__ import annotations

import csv
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

MIN_WORKERS = int(os.environ.get("MIN_WORKERS", "1"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
RUNS = int(os.environ.get("RUNS", "3"))
RUN_TIME = os.environ.get("RUN_TIME", "120s")
# Scale users with the worker count so offered load ≈ 90% of slot capacity at
# every point in the sweep, keeping the queue from building uncontrollably.
# Formula: users = workers × USERS_PER_WORKER
#   where USERS_PER_WORKER ≈ 0.9 × slots_per_worker × cycle_time / job_duration
#                           ≈ 0.9 × 4 × 2.8s / 2.0s ≈ 5
USERS_PER_WORKER = int(os.environ.get("USERS_PER_WORKER", "5"))
LOCUST_SPAWN_RATE = int(os.environ.get("LOCUST_SPAWN_RATE", "10"))
STAGE_DURATION = os.environ.get("STAGE_DURATION", "0.4")
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "loadtest-results/worker-sweep"))
API_URL = os.environ.get("API_URL", "http://localhost:8000")

LOCUST_FILE = Path(__file__).parent / "locustfile.py"
BASE_SERVICES = ["postgres", "redis", "migrate", "api", "scheduler", "scheduler-standby"]
WORKER_SERVICES = [f"worker-{i}" for i in range(1, MAX_WORKERS + 1)]


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, check=True, **kwargs)


def compose_up(*services: str) -> None:
    run(["docker", "compose", "up", "-d", "--wait", *services])


def compose_down() -> None:
    run(["docker", "compose", "down", "-v", "--remove-orphans"])


def wait_for_api(timeout: float = 120.0) -> None:
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{API_URL}/api/v1/admin/health", timeout=3)
            print("  API is healthy.", flush=True)
            return
        except (urllib.error.URLError, OSError):
            time.sleep(2)
    raise TimeoutError(f"API not reachable at {API_URL} within {timeout}s")


def run_locust(run_index: int, worker_count: int) -> dict:
    """Run Locust headlessly and return parsed stats."""
    n_users = worker_count * USERS_PER_WORKER
    prefix = RESULTS_DIR / f"w{worker_count}-r{run_index}"
    prefix.parent.mkdir(parents=True, exist_ok=True)
    csv_prefix = str(prefix)

    run(
        [
            sys.executable, "-m", "locust",
            "--headless",
            "-f", str(LOCUST_FILE),
            "--host", API_URL,
            "-u", str(n_users),
            "-r", str(min(n_users, LOCUST_SPAWN_RATE)),
            "--run-time", RUN_TIME,
            "--csv", csv_prefix,
        ],
        env={**os.environ, "ENDURE_STAGE_DURATION": STAGE_DURATION},
    )

    stats_file = Path(f"{csv_prefix}_stats.csv")
    if not stats_file.exists():
        return {}

    with stats_file.open() as f:
        rows = list(csv.DictReader(f))

    # job_completed_latency is the custom Locust event measuring wall-clock time
    # from job submission to terminal state — the correct end-to-end throughput metric.
    for row in rows:
        if row.get("Name") == "job_completed_latency":
            return {
                "requests_per_s": float(row.get("Requests/s", 0)),
                "p50_ms": float(row.get("50%", 0)),
                "p95_ms": float(row.get("95%", 0)),
                "failure_count": int(row.get("Failure Count", 0)),
            }
    return {}


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary: list[dict] = []

    for w in range(MIN_WORKERS, MAX_WORKERS + 1):
        print(f"\n{'='*60}", flush=True)
        print(f"Worker count: {w}", flush=True)

        print("Bringing up stack…", flush=True)
        worker_services = [f"worker-{i}" for i in range(1, w + 1)]
        compose_up(*BASE_SERVICES, *worker_services)
        wait_for_api()

        runs_data: list[dict] = []
        for r in range(1, RUNS + 1):
            print(f"  Run {r}/{RUNS}…", flush=True)
            stats = run_locust(r, w)
            runs_data.append(stats)
            time.sleep(2)

        compose_down()
        time.sleep(2)

        rps_values = [d["requests_per_s"] for d in runs_data if d]
        p50_values = [d["p50_ms"] for d in runs_data if d]
        p95_values = [d["p95_ms"] for d in runs_data if d]

        entry = {
            "workers": w,
            "slots": w * 4,
            "locust_users": w * USERS_PER_WORKER,
            "median_rps": median(rps_values),
            "median_p50_ms": median(p50_values),
            "median_p95_ms": median(p95_values),
            "runs": runs_data,
        }
        summary.append(entry)
        print(f"  Median RPS={entry['median_rps']:.2f}  p50={entry['median_p50_ms']:.0f}ms  (users={entry['locust_users']})", flush=True)

    out = RESULTS_DIR / "sweep_summary.json"
    out.write_text(json.dumps(summary, indent=2))
    print(f"\nSweep complete. Results: {out}", flush=True)


if __name__ == "__main__":
    main()
