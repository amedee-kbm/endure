"""E2 — Step-level recovery (RQ1 experiment).

Protocol (5 repetitions):
  1. Submit DailyImportJob (20 files × 500 rows, per-rep unique seed).
  2. Poll step_outputs (interval=0.5 s) until >= k rows (k = n_files // 2 = 10).
  3. SIGKILL the worker.
  4. Immediately read job state:
       - If COMPLETED, kill landed after archive — rep is INVALID.
         Resubmit with incremented seed and retry (up to MAX_KILL_ATTEMPTS).
       - Record how many attempts were needed.
  5. Assert recovery:
       - job reaches COMPLETED
       - RETRIED event present (proves re-dispatch, not a ghost completion)
       - final step_outputs count == n_files
       - at least KILL_AFTER_STEPS rows have created_at < t_kill,
         proving the resumed ingest replayed them from the table

Seed scheme: seed = SEED_BASE + rep * 1000.
Unique per rep so discover always finds n_files new files.

Validity assertion: step_outputs count == n_files asserted on every run.
A degenerate zero-file run (seed collision) fails loudly.

Results written to: loadtest-results/e2/<timestamp>.json
"""

import time
from pathlib import Path

import pytest

from src.evaluate import helpers as h

N_FILES = 20           # wider kill window mid-ingest (10 steps remaining after kill point)
ROWS_PER_FILE = 500
INJECT_ERRORS = 10
SEED_BASE = 20000      # E2 namespace; seed = SEED_BASE + rep * 1000
N_REPS = 5
MAX_KILL_ATTEMPTS = 3
KILL_AFTER_STEPS = N_FILES // 2   # 10


def _run_one(tenant_id: str, seed: int, date: str) -> dict:
    """
    Submit one job and attempt a mid-ingest kill after KILL_AFTER_STEPS steps.
    Returns outcome dict. 'kill_landed' is False if job completed before kill.
    """
    payload = {
        "date": date,
        "n_files": N_FILES,
        "rows_per_file": ROWS_PER_FILE,
        "seed": seed,
        "inject_errors": INJECT_ERRORS,
    }
    resp = h.submit_report(tenant_id, payload)
    job_id = str(resp["job_id"])

    # Wait until ingest is at least half-done
    h.wait_for_step_count(job_id, KILL_AFTER_STEPS, timeout=120, interval=0.5)

    # SIGKILL
    _, t_kill = h.kill_one("worker")

    # Immediately check — if COMPLETED the kill missed
    time.sleep(0.2)
    immediate_state = h.get_job(job_id)["state"]
    if immediate_state == "COMPLETED":
        return {"kill_landed": False, "job_id": job_id}

    # Kill landed — wait for recovery
    try:
        h.wait_for_worker_offline(timeout=60)
        t_offline = time.time()
    except TimeoutError:
        t_offline = None

    retried_ev = h.wait_for_event(job_id, "RETRIED", timeout=120)
    t_retried = h.epoch(retried_ev["timestamp"])

    job = h.wait_for_state(job_id, "COMPLETED", timeout=300)
    completed_ev = next(
        (e for e in reversed(h.get_events(job_id)) if e["event"] == "COMPLETED"),
        None,
    )
    t_completed = h.epoch(completed_ev["timestamp"]) if completed_ev else time.time()

    step_data = h.get_step_outputs(job_id)
    outputs = step_data.get("step_outputs", [])
    pre_kill = [
        o for o in outputs
        if o.get("created_at") and h.epoch(str(o["created_at"])) < t_kill
    ]

    return {
        "kill_landed": True,
        "job_id": job_id,
        "final_state": job["state"],
        "t_kill_epoch": t_kill,
        "t_offline_epoch": t_offline,
        "t_retried_epoch": t_retried,
        "t_completed_epoch": t_completed,
        "kill_to_offline_s": round(t_offline - t_kill, 3) if t_offline else None,
        "offline_to_retried_s": round(t_retried - t_offline, 3) if t_offline else None,
        "kill_to_completed_s": round(t_completed - t_kill, 3),
        "step_outputs_count": step_data["count"],
        "pre_kill_step_count": len(pre_kill),
        "retried_event_present": True,
    }


@pytest.mark.experiment
def test_e2_step_recovery(tenant_id: str):
    meta = h.result_metadata()
    runs: list[dict] = []
    discrepancies: list[str] = []

    for rep in range(1, N_REPS + 1):
        seed = SEED_BASE + rep * 1000
        date = f"2024-06-{rep:02d}"
        print(f"\n[E2] rep {rep}/{N_REPS} seed={seed}")

        result: dict | None = None
        for attempt in range(1, MAX_KILL_ATTEMPTS + 1):
            attempt_seed = seed + attempt - 1
            r = _run_one(tenant_id, attempt_seed, date)
            if r["kill_landed"]:
                result = {**r, "kill_attempts_needed": attempt}
                break
            print(f"[E2] rep {rep} attempt {attempt}: kill missed; retrying")

        if result is None:
            discrepancies.append(
                f"rep {rep}: kill missed all {MAX_KILL_ATTEMPTS} attempts — "
                f"raise N_FILES or ROWS_PER_FILE"
            )
            runs.append({"rep": rep, "kill_landed": False, "kill_attempts_needed": MAX_KILL_ATTEMPTS})
            continue

        print(
            f"[E2] rep {rep}: kill_to_completed={result['kill_to_completed_s']:.1f}s "
            f"(attempt {result['kill_attempts_needed']})"
        )

        # --- Validity: job must have processed n_files ---
        if result["step_outputs_count"] != N_FILES:
            discrepancies.append(
                f"rep {rep}: step_outputs={result['step_outputs_count']}, "
                f"expected {N_FILES}; possible seed collision or degenerate run"
            )

        # --- Recovery assertions ---
        if not result.get("retried_event_present"):
            discrepancies.append(f"rep {rep}: no RETRIED event — recovery not confirmed")

        if result["pre_kill_step_count"] < KILL_AFTER_STEPS:
            discrepancies.append(
                f"rep {rep}: only {result['pre_kill_step_count']} step_outputs predate kill "
                f"(expected >= {KILL_AFTER_STEPS}); resume may have re-executed steps"
            )

        runs.append({**result, "rep": rep, "seed": seed})

    out = Path(h.RESULTS_DIR) / "e2" / f"{h.ts_now()}.json"
    h.write_json(
        out,
        {
            **meta,
            "n_reps": N_REPS,
            "n_files": N_FILES,
            "rows_per_file": ROWS_PER_FILE,
            "kill_after_steps": KILL_AFTER_STEPS,
            "seed_base": SEED_BASE,
            "runs": runs,
        },
    )

    if discrepancies:
        pytest.fail(
            f"E2: {len(discrepancies)} assertion(s) failed:\n"
            + "\n".join(f"  • {d}" for d in discrepancies)
        )
