"""E4 — Worker-count throughput sweep (RQ2 experiment).

Submits N_JOBS=20 identical jobs simultaneously and waits for all to complete.
Does NOT control worker scaling — the stack must already be running at the
correct count (set by run_e4_sweep.sh via --scale worker=N).

Worker count is read from ENDURE_E4_WORKERS env var (default: 1).

Seed scheme: seed = SEED_BASE + (rep - 1) * N_JOBS + job_index
Every (rep, job) pair gets a unique seed so discover always finds N_FILES
new files, even across the 3 repetitions run without a DB wipe between reps.

Validity assertion: each completed job must have step_outputs count == N_FILES.
A degenerate zero-file run (seed collision) is counted as a discrepancy.

Per configuration (3 reps):
  - Submits N_JOBS simultaneously
  - Measures makespan = last completed_at − first created_at
  - Records per-job duration (started_at → completed_at)

Results written to: loadtest-results/e4/w{N}_{timestamp}.csv
One CSV row per (worker_count, rep, job).
"""

import os
import time
from pathlib import Path

import pytest

from src.evaluate import helpers as h

N_JOBS = 20
N_FILES = 10
ROWS_PER_FILE = 500
INJECT_ERRORS = 5
SEED_BASE = 40000   # E4 namespace; seed = SEED_BASE + (rep-1)*N_JOBS + job_index
N_REPS = 3


@pytest.mark.experiment
def test_e4_worker_sweep(tenant_id: str):
    worker_count = int(os.environ.get("ENDURE_E4_WORKERS", "1"))
    print(f"\n[E4] worker_count={worker_count}, n_jobs={N_JOBS}, n_reps={N_REPS}")

    meta = h.result_metadata()
    csv_rows: list[dict] = []
    discrepancies: list[str] = []

    for rep in range(1, N_REPS + 1):
        print(f"[E4] rep {rep}/{N_REPS} (workers={worker_count})")

        # Submit all N_JOBS simultaneously; each gets a globally unique seed
        job_ids: list[str] = []
        t_submit_start = time.time()
        for i in range(N_JOBS):
            seed = SEED_BASE + (rep - 1) * N_JOBS + i
            payload = {
                "n_files": N_FILES,
                "rows_per_file": ROWS_PER_FILE,
                "seed": seed,
                "inject_errors": INJECT_ERRORS,
                "date": f"2024-08-{(rep - 1) * N_JOBS + i + 1:02d}",
            }
            resp = h.submit_report(tenant_id, payload)
            job_ids.append(str(resp["job_id"]))
        t_submit_end = time.time()
        print(f"[E4] submitted {N_JOBS} jobs in {t_submit_end - t_submit_start:.2f}s")

        # Wait for all to reach a terminal state
        terminal: dict[str, dict] = {}
        deadline = time.monotonic() + 600
        while len(terminal) < N_JOBS and time.monotonic() < deadline:
            for jid in job_ids:
                if jid not in terminal:
                    j = h.get_job(jid)
                    if j["state"] in h.TERMINAL_STATES:
                        terminal[jid] = j
            time.sleep(2)

        if len(terminal) < N_JOBS:
            discrepancies.append(
                f"rep {rep} w={worker_count}: only {len(terminal)}/{N_JOBS} jobs "
                f"reached terminal state within timeout"
            )

        completed_jobs = [j for j in terminal.values() if j["state"] == "COMPLETED"]
        if not completed_jobs:
            discrepancies.append(f"rep {rep} w={worker_count}: no COMPLETED jobs")
            continue

        # Validity: each completed job must have processed N_FILES
        for j in completed_jobs:
            jid = str(j["id"])
            sc = h.get_step_outputs(jid)["count"]
            if sc != N_FILES:
                discrepancies.append(
                    f"rep {rep} w={worker_count} job {jid}: "
                    f"step_outputs={sc}, expected {N_FILES}; "
                    f"possible seed collision — run is invalid"
                )

        first_created = min(h.epoch(j["created_at"]) for j in completed_jobs)
        last_completed = max(
            h.epoch(j["completed_at"])
            for j in completed_jobs
            if j.get("completed_at")
        )
        makespan_s = round(last_completed - first_created, 3)
        jobs_per_min = round(len(completed_jobs) / makespan_s * 60, 2)

        print(
            f"[E4] rep {rep} w={worker_count}: makespan={makespan_s:.1f}s, "
            f"{jobs_per_min:.1f} jobs/min, {len(completed_jobs)}/{N_JOBS} completed"
        )

        for j in terminal.values():
            started = j.get("started_at")
            completed = j.get("completed_at")
            duration_s = (
                round(h.epoch(completed) - h.epoch(started), 3)
                if started and completed
                else None
            )
            csv_rows.append({
                "git_commit": meta["git_commit"],
                "timestamp": meta["timestamp"],
                "worker_count": worker_count,
                "rep": rep,
                "job_id": j["id"],
                "state": j["state"],
                "duration_s": duration_s,
                "makespan_s": makespan_s,
                "jobs_per_min": jobs_per_min,
                **{f"setting_{k}": v for k, v in meta["settings"].items()},
            })

    out = Path(h.RESULTS_DIR) / "e4" / f"w{worker_count}_{h.ts_now()}.csv"
    h.write_csv(out, csv_rows)

    if discrepancies:
        pytest.fail(
            f"E4 w={worker_count}: {len(discrepancies)} issue(s):\n"
            + "\n".join(f"  • {d}" for d in discrepancies)
        )
