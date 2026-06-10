"""
RQ1 scalability sweep using k6 (constant-arrival-rate / open model).

Starts the base stack once, adds workers one at a time, and fires a k6
constant-arrival-rate burst at each worker count. k6 measures the actual
completion rate — the system itself acts as the rate limiter.

Two modes:
  FIXED_RATE set  — same arrival rate for every worker count; system saturates
                    at low counts and the actual throughput is the measurement.
                    Queue is flushed between counts to avoid contamination.
  FIXED_RATE unset — rate scales with worker count (RATE_PER_WORKER × w);
                    confirms the system sustains proportional load.

Usage:
    uv run python src/evaluate/load/run_k6_sweep.py

Environment variables:
    MAX_WORKERS      highest worker count to sweep (default 8)
    FIXED_RATE       fixed arrival rate for all counts, jobs/s (optional)
                       when set, measures actual throughput ceiling per count
    RATE_PER_WORKER  arrival rate per worker when FIXED_RATE not set (default 1.3)
    WARMUP_SECS      k6 warm-up window excluded from stats (default 5)
    DURATION_SECS    k6 measurement window per count (default 30)
    STAGE_DURATION   seconds per synthetic job stage (default 0.4)
    RESULTS_DIR      output directory (default loadtest-results/worker-sweep-k6)
    API_URL          endure API (default http://localhost:8000)
"""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
K6_SCRIPT = ROOT / "src" / "evaluate" / "load" / "k6" / "script.js"

# Locate k6 — winget installs to a fixed path that may not be in subprocess PATH.
_K6_CANDIDATES = [
    r"C:\Program Files\k6\k6.exe",
    r"C:\ProgramData\chocolatey\bin\k6.exe",
]

def _find_k6() -> str:
    k6 = shutil.which("k6")
    if k6:
        return k6
    for c in _K6_CANDIDATES:
        if Path(c).exists():
            return c
    raise FileNotFoundError(
        "k6 not found. Install from https://grafana.com/docs/k6/latest/set-up/install-k6/"
    )

K6 = _find_k6()
BASE_SERVICES = ["postgres", "redis", "migrate", "api", "scheduler", "scheduler-standby"]

API_URL         = os.environ.get("API_URL", "http://localhost:8000")
MIN_WORKERS     = int(os.environ.get("MIN_WORKERS", "1"))
MAX_WORKERS     = int(os.environ.get("MAX_WORKERS", "8"))
FIXED_RATE      = float(os.environ.get("FIXED_RATE", "0")) or None   # None = proportional mode
RATE_PER_WORKER = float(os.environ.get("RATE_PER_WORKER", "1.3"))
WARMUP_SECS     = int(os.environ.get("WARMUP_SECS", "5"))
DURATION_SECS   = int(os.environ.get("DURATION_SECS", "30"))
STAGE_DURATION  = os.environ.get("STAGE_DURATION", "0.4")
USE_ISOLATION   = os.environ.get("ENDURE_USE_PROCESS_ISOLATION", "false")
RESULTS_DIR     = Path(os.environ.get("RESULTS_DIR", "loadtest-results/worker-sweep-k6"))


def sh(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(str(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=check, cwd=ROOT)


def wait_for_api(timeout: int = 120) -> None:
    import urllib.request, urllib.error
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{API_URL}/api/v1/admin/health", timeout=3)
            print("  API healthy.", flush=True)
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError("API not reachable")


def run_k6(worker_count: int, rate: float, out_json: Path) -> dict | None:
    cmd = [
        K6, "run",
        "--env", f"API_URL={API_URL}",
        "--env", f"RATE={rate:.2f}",
        "--env", f"DURATION={DURATION_SECS}",
        "--env", f"WARMUP={WARMUP_SECS}",
        "--env", f"STAGE_DURATION={STAGE_DURATION}",
        "--env", f"TENANT_NAME=k6-tenant-w{worker_count}",
        "--summary-export", str(out_json),
        "--no-color",
        str(K6_SCRIPT),
    ]
    result = sh(cmd, check=False)

    if not out_json.exists():
        print(f"  [!] No k6 summary at {out_json}", flush=True)
        return None

    with out_json.open() as f:
        data = json.load(f)

    metrics = data.get("metrics", {})

    def get_val(metric: str, stat: str) -> float | None:
        # k6 v2 --summary-export: stats sit directly on the metric object,
        # no intermediate "values" wrapper (changed from older k6 versions).
        m = metrics.get(metric, {})
        return m.get(stat)

    # Use count/DURATION_SECS for throughput — cleaner than the overall "rate"
    # which is diluted by the gracefulStop window after the scenario ends.
    count = get_val("iterations", "count")
    jobs_per_s = (count / DURATION_SECS) if count is not None else None

    return {
        "jobs_per_s":     jobs_per_s,
        "p50_ms":         get_val("iteration_duration", "med"),
        "p95_ms":         get_val("iteration_duration", "p(95)"),
        "p99_ms":         get_val("iteration_duration", "p(99)"),
        "dropped":        get_val("dropped_iterations", "count") or 0,
        "k6_exit_code":   result.returncode,
    }


def _cleanup_queue() -> None:
    """Flush queued jobs from Redis and DB so leftover overflow jobs from a
    high-rate run don't contaminate the next worker count's measurement."""
    # Remove all entries from the job queue sorted set
    sh(["docker", "exec", "endure-redis-1", "redis-cli", "DEL", "endure:queue:jobs"], check=False)
    # Wipe all job rows (events and DLQ cascade)
    sh([
        "docker", "exec", "endure-api",
        "python", "manage.py", "shell", "-c",
        (
            "from src.models import Job, JobEvent, DeadLetterJob; "
            "DeadLetterJob.objects.all().delete(); "
            "JobEvent.objects.all().delete(); "
            "Job.objects.all().delete()"
        ),
    ], check=False)
    time.sleep(2)  # let the scheduler notice the empty queue


def _wait_for_worker(worker_num: int, warmup_secs: int = 8) -> None:
    """Wait for the worker to register, then prime the API connection pool
    with a real job so the measurement window starts from a warm system."""
    import urllib.request, urllib.error, json as _json
    time.sleep(warmup_secs)
    # Submit one warmup job (fire-and-forget — just warms up connections)
    try:
        body = _json.dumps({
            "name": f"warmup-w{worker_num}",
            "tenant_id": None,   # will fail gracefully — that's fine
            "job_type": "src.evaluate.jobs:SyntheticJob",
            "payload": {"stage_duration": 0.1, "stages": 1},
            "max_retries": 0, "timeout_seconds": 10,
        }).encode()
        req = urllib.request.Request(
            f"{API_URL}/api/v1/jobs",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass   # warmup job may fail (no tenant) — connection pool is primed regardless
    time.sleep(2)  # let scheduler dispatch the warmup job before k6 starts


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()

    # Propagate isolation flag so docker-compose substitution picks it up
    os.environ["ENDURE_USE_PROCESS_ISOLATION"] = USE_ISOLATION

    print(f"Building image (isolation={USE_ISOLATION})...", flush=True)
    sh(["docker", "compose", "build"])

    print("Starting base stack (one-time)...", flush=True)
    sh(["docker", "compose", "up", "-d", "--wait", *BASE_SERVICES])
    wait_for_api()

    results = []

    for w in range(MIN_WORKERS, MAX_WORKERS + 1):
        rate = FIXED_RATE if FIXED_RATE else w * RATE_PER_WORKER
        mode = "fixed" if FIXED_RATE else f"{RATE_PER_WORKER}/worker"

        print(f"\n{'='*60}", flush=True)
        print(f"[w={w}]  Scaling to {w} worker(s)  |  rate={rate:.1f} jobs/s ({mode})", flush=True)
        sh(["docker", "compose", "up", "-d", "--no-deps", "--scale", f"worker={w}", "worker"])
        _wait_for_worker(w)

        out_json = RESULTS_DIR / f"w{w}.json"
        stats = run_k6(w, rate, out_json)

        if FIXED_RATE:
            _cleanup_queue()

        if stats:
            rps  = f"{stats['jobs_per_s']:.2f}"  if stats.get('jobs_per_s')  is not None else "N/A"
            p50  = f"{stats['p50_ms']:.0f}ms"    if stats.get('p50_ms')      is not None else "N/A"
            p95  = f"{stats['p95_ms']:.0f}ms"    if stats.get('p95_ms')      is not None else "N/A"
            drop = stats.get('dropped', 0)
            print(f"[w={w}]  {rps} jobs/s  p50={p50}  p95={p95}  dropped={drop}", flush=True)
        else:
            print(f"[w={w}]  no results", flush=True)

        results.append({
            "workers":     w,
            "slots":       w * 4,
            "target_rate": rate,
            **(stats or {}),
        })

    sh(["docker", "compose", "down", "-v"], check=False)
    elapsed = time.monotonic() - t0

    summary_path = RESULTS_DIR / "summary.json"
    summary_path.write_text(json.dumps(results, indent=2))

    print(f"\n{'='*66}", flush=True)
    print(
        f"{'Workers':>8} {'Slots':>6} {'Target':>8} {'Actual':>8}"
        f" {'p50':>8} {'p95':>8} {'Dropped':>8}",
        flush=True,
    )
    print("-" * 66, flush=True)
    for r in results:
        rps  = f"{r['jobs_per_s']:.2f}"  if r.get('jobs_per_s')  is not None else "N/A"
        p50  = f"{r['p50_ms']:.0f}"      if r.get('p50_ms')      is not None else "N/A"
        p95  = f"{r['p95_ms']:.0f}"      if r.get('p95_ms')      is not None else "N/A"
        drop = r.get('dropped', 0) or 0
        print(
            f"{r['workers']:>8} {r['slots']:>6} {r['target_rate']:>8.1f}"
            f" {rps:>8} {p50:>7}ms {p95:>7}ms {drop:>8}",
            flush=True,
        )

    print(f"\nTotal elapsed: {elapsed:.0f}s  |  Results: {summary_path}", flush=True)


if __name__ == "__main__":
    main()
