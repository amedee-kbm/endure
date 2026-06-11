"""E1 — Stage-level recovery (RQ1 experiment).

Protocol (5 repetitions):
  1. Submit DailyImportJob (10 files × 2000 rows per-rep unique seed).
  2. Poll checkpoints (interval=0.5 s) until sequence_number >= 3
     (discover + ingest + validate all checkpointed).
  3. SIGKILL the worker.
  4. Immediately read job state:
       - If COMPLETED, the kill landed after archive finished — rep is INVALID.
         Resubmit with the same parameters and try again (up to MAX_KILL_ATTEMPTS).
       - Record how many attempts were needed.
  5. Assert recovery:
       - job reaches COMPLETED
       - RETRIED event present (proves re-dispatch happened, not a ghost completion)
       - resume RUNNING detail contains "Skipping N completed stage(s)" naming
         discover, ingest, validate
       - step_outputs count == n_files (unique constraint prevents re-recording)

Seed scheme: seed = SEED_BASE + rep * 1000.
Unique per rep → discover always finds n_files new files even across reruns.

Validity assertion: step_outputs count == n_files asserted on EVERY run;
a degenerate zero-file run fails loudly instead of silently passing.

Results written to: loadtest-results/e1/<timestamp>.json
"""

import time
from pathlib import Path

import pytest

from src.evaluate import helpers as h

N_FILES = 10
ROWS_PER_FILE = 2000   # large enough to widen the kill window (transform + Excel render)
INJECT_ERRORS = 10
SEED_BASE = 10000      # E1 namespace; seed = SEED_BASE + rep * 1000
N_REPS = 5
MAX_KILL_ATTEMPTS = 3  # per rep: if kill misses, resubmit and retry
CHECKPOINT_SEQ_THRESHOLD = 3  # discover(1) + ingest(2) + validate(3)


def _run_one(tenant_id: str, seed: int, date: str) -> dict:
    """
    Submit one job and attempt a mid-execution kill after validate is checkpointed.
    Returns a dict with outcome fields. 'kill_landed' is False if the job
    completed before the kill could take effect.
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

    # Validity pre-check: poll until job is at least RUNNING before we start timing
    h.wait_for_state(job_id, {"QUEUED", "SCHEDULED", "RUNNING"}, timeout=60, interval=1.0)

    # Wait for checkpoint seq >= 3 (validate stage completed and checkpointed)
    # Use short interval so we catch it before transform/archive finish.
    h.wait_for_checkpoint(job_id, min_seq=CHECKPOINT_SEQ_THRESHOLD, timeout=240, interval=0.5)

    # SIGKILL
    _, t_kill = h.kill_one("worker")

    # Immediately check state — if COMPLETED the kill missed
    time.sleep(0.2)
    immediate_state = h.get_job(job_id)["state"]
    if immediate_state == "COMPLETED":
        return {"kill_landed": False, "job_id": job_id}

    # Kill landed — now wait for recovery
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

    events = h.get_events(job_id)
    retried_ts = retried_ev["timestamp"]
    resume_running = next(
        (e for e in events if e["event"] == "RUNNING" and e["timestamp"] > retried_ts),
        None,
    )
    step_count = h.get_step_outputs(job_id)["count"]

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
        "step_outputs_count": step_count,
        "resume_running_detail": (resume_running or {}).get("detail", ""),
        "retried_event_present": True,
    }


@pytest.mark.experiment
def test_e1_stage_recovery(tenant_id: str):
    meta = h.result_metadata()
    runs: list[dict] = []
    discrepancies: list[str] = []

    for rep in range(1, N_REPS + 1):
        seed = SEED_BASE + rep * 1000
        date = f"2024-05-{rep:02d}"
        print(f"\n[E1] rep {rep}/{N_REPS} seed={seed}")

        result: dict | None = None
        for attempt in range(1, MAX_KILL_ATTEMPTS + 1):
            # Each kill attempt uses an incremented seed so discover finds fresh files
            attempt_seed = seed + attempt - 1
            r = _run_one(tenant_id, attempt_seed, date)
            if r["kill_landed"]:
                result = {**r, "kill_attempts_needed": attempt}
                break
            print(f"[E1] rep {rep} attempt {attempt}: kill missed (job completed first); retrying")

        if result is None:
            discrepancies.append(
                f"rep {rep}: kill missed all {MAX_KILL_ATTEMPTS} attempts — "
                f"raise ROWS_PER_FILE or increase kill-window"
            )
            runs.append({"rep": rep, "kill_landed": False, "kill_attempts_needed": MAX_KILL_ATTEMPTS})
            continue

        job_id = result["job_id"]
        print(
            f"[E1] rep {rep}: kill_to_completed={result['kill_to_completed_s']:.1f}s "
            f"(attempt {result['kill_attempts_needed']})"
        )

        # --- Validity: job must have processed n_files (not a degenerate zero-file run) ---
        if result["step_outputs_count"] != N_FILES:
            discrepancies.append(
                f"rep {rep}: step_outputs={result['step_outputs_count']}, "
                f"expected {N_FILES}; possible seed collision or degenerate run"
            )

        # --- Recovery assertions ---
        if not result.get("retried_event_present"):
            discrepancies.append(f"rep {rep}: no RETRIED event — recovery not confirmed")

        detail = result.get("resume_running_detail", "")
        if "Skipping" not in detail:
            discrepancies.append(
                f"rep {rep}: resume RUNNING detail missing 'Skipping': {detail!r}"
            )
        for stage in ("discover", "ingest", "validate"):
            if stage not in detail:
                discrepancies.append(
                    f"rep {rep}: stage {stage!r} not named in skip detail: {detail!r}"
                )

        runs.append({**result, "rep": rep, "seed": seed})

    out = Path(h.RESULTS_DIR) / "e1" / f"{h.ts_now()}.json"
    h.write_json(
        out,
        {
            **meta,
            "n_reps": N_REPS,
            "n_files": N_FILES,
            "rows_per_file": ROWS_PER_FILE,
            "seed_base": SEED_BASE,
            "runs": runs,
        },
    )

    if discrepancies:
        pytest.fail(
            f"E1: {len(discrepancies)} assertion(s) failed:\n"
            + "\n".join(f"  • {d}" for d in discrepancies)
        )
