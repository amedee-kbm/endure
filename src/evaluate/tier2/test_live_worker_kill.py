"""
Tier 2 — ONE live worker-kill demonstration (full Docker stack required).

Not a measurement campaign: detection latency is an analytic bound
(heartbeat timeout + ≤ one heartbeat interval + ≤ one sweep interval), whose
logic Tier 1 pins by state injection. This single end-to-end run shows the
loop closes in reality and reports the one observed latency against the bound.

Run:  dc run --rm runner pytest src/evaluate/tier2/test_live_worker_kill.py -v
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from src.evaluate import helpers as H

pytestmark = pytest.mark.live

SEED = 60001
N_FILES = 10
ROWS = 2000  # wide ingest window so the kill lands mid-run

OUT = Path("loadtest-results/tier2")


def test_live_worker_kill_end_to_end():
    cfg = H.capture_settings()
    bound = (
        cfg["worker_heartbeat_timeout"]
        + cfg["worker_heartbeat_interval"]
        + cfg["scheduler_loop_interval"]
    )

    containers = H.find_service_containers("worker")
    assert len(containers) == 2, (
        f"expected 2 worker containers, found {len(containers)}; "
        "was the stack reconciled? run with --no-deps and --scale worker=2"
    )

    tenant = H.ensure_tenant("tier2")
    job = H.submit_report(
        tenant_id=tenant["id"], report_type="daily_import",
        payload={"n_files": N_FILES, "rows_per_file": ROWS,
                 "seed": SEED, "inject_errors": 0},
    )
    job_id = job["job_id"]

    H.wait_for_state(job_id, "RUNNING", timeout=60)
    H.wait_for_checkpoint(job_id, min_count=1, timeout=60)  # past discover

    _, t_kill = H.kill_worker_owning(job_id)
    state = H.get_job(job_id)["state"]
    assert state in ("RUNNING", "QUEUED", "SCHEDULED"), (
        f"kill landed too late (state={state}); raise ROWS and rerun"
    )

    offline = H.wait_for_worker_offline(timeout=bound + 10)
    t_offline = time.time()

    H.wait_for_event(job_id, "RETRIED", timeout=30)
    H.wait_for_state(job_id, "COMPLETED", timeout=300)
    t_done = time.time()

    steps = H.get_step_outputs(job_id)
    result = {
        **H.result_metadata(),
        "analytic_detection_bound_s": round(bound, 2),
        "observed_kill_to_offline_s": round(t_offline - t_kill, 2),
        "observed_kill_to_completed_s": round(t_done - t_kill, 2),
        "offline_worker": offline.get("id"),
        "step_outputs_count": steps.get("count", len(steps.get("items", []))),
        "events": [e["event"] for e in H.get_events(job_id)],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    H.write_json(OUT / f"worker_kill_{H.ts_now()}.json", result)

    assert result["observed_kill_to_offline_s"] <= bound + 5.0, (
        "detection exceeded the analytic bound — investigate before writing it up"
    )
    assert result["step_outputs_count"] == N_FILES
    assert "RETRIED" in result["events"]
