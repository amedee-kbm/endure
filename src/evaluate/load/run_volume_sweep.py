"""
RQ1 data-volume sweep — measures DailySalesReportJob completion time
across n_orders values with a fixed worker count.

Produces Table 4.1: Pipeline completion time vs. data volume.

Usage:
    uv run python src/evaluate/load/run_volume_sweep.py

Environment variables:
    WORKERS         fixed worker count to run with (default 4)
    RUNS            runs per volume level for median (default 5)
    N_ORDERS        comma-separated list of row counts to sweep
                    (default 200,1000,5000,20000,100000)
    SEED            base random seed; incremented per run for variety (default 1)
    API_URL         endure API base URL (default http://localhost:8000)
    RESULTS_DIR     output directory (default loadtest-results/volume-sweep)
    TENANT_NAME     tenant name to create or reuse (default volume-sweep-tenant)
    POLL_INTERVAL   seconds between job status polls (default 2.0)
    JOB_TIMEOUT     max seconds to wait for a single job (default 600)
    MANAGE_STACK    set to 1 to let this script start/stop docker compose
                    (default 0 — assumes the stack is already running)
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

API_URL       = os.environ.get("API_URL", "http://localhost:8000").rstrip("/")
WORKERS       = int(os.environ.get("WORKERS", "4"))
RUNS          = int(os.environ.get("RUNS", "5"))
SEED_BASE     = int(os.environ.get("SEED", "1"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
JOB_TIMEOUT   = float(os.environ.get("JOB_TIMEOUT", "600"))
TENANT_NAME   = os.environ.get("TENANT_NAME", "volume-sweep-tenant")
RESULTS_DIR   = Path(os.environ.get("RESULTS_DIR", "loadtest-results/volume-sweep"))
MANAGE_STACK  = os.environ.get("MANAGE_STACK", "0").strip() in ("1", "true", "yes")

_raw = os.environ.get("N_ORDERS", "200,1000,5000,20000,100000")
N_ORDERS_LIST = [int(x.strip()) for x in _raw.split(",") if x.strip()]

TERMINAL = {"COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED", "TIMED_OUT"}

BASE_SERVICES = ["postgres", "redis", "migrate", "api", "scheduler", "scheduler-standby"]


# ---------------------------------------------------------------------------
# HTTP helpers (plain urllib — no third-party dependencies for the sweep driver)
# ---------------------------------------------------------------------------

def _request(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{API_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc.code} {exc.read()[:200]}")


def wait_for_api(timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{API_URL}/api/v1/admin/health", timeout=3)
            print("  API healthy.", flush=True)
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError(f"API not reachable at {API_URL} within {timeout}s")


def resolve_tenant() -> str:
    try:
        result = _request("POST", "/api/v1/admin/tenants", {
            "name": TENANT_NAME,
            "max_concurrent_jobs": 100,
            "max_workers": WORKERS + 2,
        })
        return str(result["id"])
    except RuntimeError as exc:
        if "409" in str(exc):
            tenants = _request("GET", "/api/v1/admin/tenants")
            for t in tenants:
                if t["name"] == TENANT_NAME:
                    return str(t["id"])
        raise


def submit_report(tenant_id: str, n_orders: int, seed: int) -> str:
    result = _request("POST", "/api/v1/reports", {
        "tenant_id": tenant_id,
        "report_type": "daily_sales",
        "payload": {
            "date": "2026-06-01",
            "n_orders": n_orders,
            "seed": seed,
        },
        "max_retries": 0,
        "timeout_seconds": int(JOB_TIMEOUT),
    })
    return str(result["job_id"])


def poll_job(job_id: str) -> dict:
    return _request("GET", f"/api/v1/jobs/{job_id}")


def wait_for_completion(job_id: str) -> tuple[str, float]:
    """Poll until terminal. Returns (final_state, elapsed_seconds)."""
    start = time.monotonic()
    deadline = start + JOB_TIMEOUT
    while time.monotonic() < deadline:
        job = poll_job(job_id)
        state = job.get("state", "")
        if state in TERMINAL:
            return state, time.monotonic() - start
        time.sleep(POLL_INTERVAL)
    return "TIMED_OUT", time.monotonic() - start


# ---------------------------------------------------------------------------
# Docker helpers (only used when MANAGE_STACK=1)
# ---------------------------------------------------------------------------

def sh(cmd: list[str]) -> None:
    print(f"  $ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def stack_up() -> None:
    sh(["docker", "compose", "up", "-d", "--wait", "--scale", f"worker={WORKERS}"])


def stack_down() -> None:
    sh(["docker", "compose", "down", "-v", "--remove-orphans"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_v = sorted(values)
    idx = (len(sorted_v) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_v) - 1)
    return sorted_v[lo] + (sorted_v[hi] - sorted_v[lo]) * (idx - lo)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if MANAGE_STACK:
        print(f"Starting stack with {WORKERS} worker(s)...", flush=True)
        stack_up()

    wait_for_api()
    tenant_id = resolve_tenant()
    print(f"Tenant: {tenant_id}", flush=True)

    summary: list[dict] = []

    for n in N_ORDERS_LIST:
        print(f"\n{'='*60}", flush=True)
        print(f"n_orders={n}", flush=True)

        times: list[float] = []
        states: list[str] = []

        for run_idx in range(1, RUNS + 1):
            seed = SEED_BASE + run_idx - 1
            print(f"  run {run_idx}/{RUNS}  seed={seed}...", end=" ", flush=True)
            job_id = submit_report(tenant_id, n, seed)
            state, elapsed = wait_for_completion(job_id)
            print(f"{state}  {elapsed:.1f}s", flush=True)
            times.append(elapsed)
            states.append(state)

        completed_times = [t for t, s in zip(times, states) if s == "COMPLETED"]
        p50 = percentile(completed_times, 50) if completed_times else None
        p95 = percentile(completed_times, 95) if completed_times else None

        entry = {
            "n_orders": n,
            "runs": RUNS,
            "completed": sum(1 for s in states if s == "COMPLETED"),
            "p50_s": round(p50, 2) if p50 is not None else None,
            "p95_s": round(p95, 2) if p95 is not None else None,
            "median_s": round(statistics.median(completed_times), 2) if completed_times else None,
            "times_s": [round(t, 2) for t in times],
            "states": states,
        }
        summary.append(entry)
        print(f"  n_orders={n}  p50={p50:.1f}s  p95={p95:.1f}s  completed={entry['completed']}/{RUNS}", flush=True)

    if MANAGE_STACK:
        stack_down()

    out = RESULTS_DIR / "volume_summary.json"
    out.write_text(json.dumps(summary, indent=2))

    print(f"\n{'='*60}", flush=True)
    print(f"{'n_orders':>10}  {'p50 (s)':>8}  {'p95 (s)':>8}  {'ok':>4}", flush=True)
    print("-" * 40, flush=True)
    for row in summary:
        p50 = f"{row['p50_s']:.1f}" if row["p50_s"] is not None else "N/A"
        p95 = f"{row['p95_s']:.1f}" if row["p95_s"] is not None else "N/A"
        print(f"{row['n_orders']:>10}  {p50:>8}  {p95:>8}  {row['completed']:>2}/{row['runs']}", flush=True)

    print(f"\nResults: {out}", flush=True)


if __name__ == "__main__":
    main()
