"""E3 — Coordinator failover (RQ1 experiment).

Protocol (5 repetitions):
  1. Identify the current leader via GET /admin/leader.
  2. Submit a DailyImportJob (per-rep unique seed).
  3. SIGKILL the leader scheduler container while the job is in flight.
  4. Wait for a new holder_id in scheduler_leader (standby acquires lease).
  5. Wait for COMPLETED.
  6. Assert no duplicate SCHEDULED assignment of the same job to two workers.
  7. Restart the killed container so the next rep has a full two-node cluster.

Seed scheme: seed = SEED_BASE + rep * 1000.
Unique per rep so discover always finds n_files new files.

Validity assertion: step_outputs count == n_files asserted on every run.

Timing recorded per run:
  - t_kill            local UTC epoch when SIGKILL sent
  - t_acquired_epoch  UTC epoch from new_leader.acquired_at
  - t_completed_epoch UTC epoch from COMPLETED event
  - kill_to_acquired_s
  - kill_to_completed_s

Also records LEADER_LOCK_TTL and LEADER_HEARTBEAT_INTERVAL.

Results written to: loadtest-results/e3/<timestamp>.json
"""

import time
from pathlib import Path

import pytest

from src.evaluate import helpers as h

N_FILES = 8
ROWS_PER_FILE = 300
INJECT_ERRORS = 3
SEED_BASE = 30000      # E3 namespace; seed = SEED_BASE + rep * 1000
N_REPS = 5


@pytest.mark.experiment
def test_e3_coordinator_failover(tenant_id: str):
    meta = h.result_metadata()
    runs: list[dict] = []
    discrepancies: list[str] = []

    for rep in range(1, N_REPS + 1):
        seed = SEED_BASE + rep * 1000
        date = f"2024-07-{rep:02d}"
        print(f"\n[E3] rep {rep}/{N_REPS} seed={seed}")

        leader = h.get_leader()
        assert leader, (
            "No scheduler leader found — ensure both scheduler containers are running"
        )
        old_holder = leader["holder_id"]
        leader_container = h.holder_to_container(old_holder)
        print(f"[E3] rep {rep}: leader={old_holder}")

        payload = {
            "date": date,
            "n_files": N_FILES,
            "rows_per_file": ROWS_PER_FILE,
            "seed": seed,
            "inject_errors": INJECT_ERRORS,
        }
        resp = h.submit_report(tenant_id, payload)
        job_id = str(resp["job_id"])

        # Brief pause so the job enters the scheduler's queue before the kill
        time.sleep(1.0)

        t_kill = h.kill_named(leader_container)
        print(f"[E3] rep {rep}: killed {leader_container}")

        # Wait for standby to acquire the lease
        try:
            new_leader = h.wait_for_leader_change(old_holder, timeout=90)
            t_acquired_epoch = h.epoch(new_leader["acquired_at"])
            new_holder = new_leader["holder_id"]
            print(f"[E3] rep {rep}: new leader={new_holder}")
        except TimeoutError as exc:
            discrepancies.append(f"rep {rep}: leader never changed: {exc}")
            t_acquired_epoch = None
            new_holder = None

        job = h.wait_for_state(job_id, "COMPLETED", timeout=300)
        completed_ev = next(
            (e for e in reversed(h.get_events(job_id)) if e["event"] == "COMPLETED"),
            None,
        )
        t_completed = h.epoch(completed_ev["timestamp"]) if completed_ev else time.time()

        # --- Correctness assertions ---
        events = h.get_events(job_id)

        # No duplicate dispatch: at most 1 SCHEDULED event
        scheduled_events = [e for e in events if e["event"] == "SCHEDULED"]
        if len(scheduled_events) > 1:
            worker_ids = [e.get("worker_id") for e in scheduled_events]
            discrepancies.append(
                f"rep {rep}: {len(scheduled_events)} SCHEDULED events "
                f"(possible duplicate dispatch); worker_ids={worker_ids}"
            )

        if job["state"] != "COMPLETED":
            discrepancies.append(
                f"rep {rep}: job did not complete, final state={job['state']!r}"
            )

        if new_holder is None:
            discrepancies.append(f"rep {rep}: leader change timed out")
        elif new_holder == old_holder:
            discrepancies.append(
                f"rep {rep}: holder_id unchanged after kill ({old_holder!r})"
            )

        # --- Validity: job must have processed n_files ---
        step_count = h.get_step_outputs(job_id)["count"]
        if step_count != N_FILES:
            discrepancies.append(
                f"rep {rep}: step_outputs={step_count}, expected {N_FILES}; "
                f"possible seed collision or degenerate run"
            )

        run = {
            "rep": rep,
            "seed": seed,
            "job_id": job_id,
            "killed_holder": old_holder,
            "new_holder": new_holder,
            "final_state": job["state"],
            "step_outputs_count": step_count,
            "t_kill_epoch": t_kill,
            "t_acquired_epoch": t_acquired_epoch,
            "t_completed_epoch": t_completed,
            "kill_to_acquired_s": (
                round(t_acquired_epoch - t_kill, 3) if t_acquired_epoch else None
            ),
            "kill_to_completed_s": round(t_completed - t_kill, 3),
            "scheduled_event_count": len(scheduled_events),
        }
        runs.append(run)
        print(
            f"[E3] rep {rep}: kill_to_acquired={run['kill_to_acquired_s']}s, "
            f"kill_to_completed={run['kill_to_completed_s']:.1f}s"
        )

        # Restart the killed scheduler so the next rep has a full two-node cluster
        try:
            import docker as _docker
            _docker.from_env().containers.get(leader_container).restart()
            time.sleep(5)  # let it re-register before next rep
        except Exception as exc:
            print(f"[E3] rep {rep}: warning — could not restart {leader_container}: {exc}")

    out = Path(h.RESULTS_DIR) / "e3" / f"{h.ts_now()}.json"
    h.write_json(
        out,
        {
            **meta,
            "n_reps": N_REPS,
            "n_files": N_FILES,
            "rows_per_file": ROWS_PER_FILE,
            "seed_base": SEED_BASE,
            "configured_leader_lock_ttl_s": h.capture_settings()["leader_lock_ttl"],
            "configured_leader_heartbeat_interval_s": h.capture_settings()[
                "leader_heartbeat_interval"
            ],
            "runs": runs,
        },
    )

    if discrepancies:
        pytest.fail(
            f"E3: {len(discrepancies)} assertion(s) failed:\n"
            + "\n".join(f"  • {d}" for d in discrepancies)
        )
