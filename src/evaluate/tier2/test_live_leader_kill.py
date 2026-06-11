"""
Tier 2 — ONE live coordinator-failover demonstration.

Requires both scheduler containers up. Shows: leader killed mid-flight, the
standby acquires within the lease bound, the in-flight job completes with no
duplicate dispatch.

Run:  dc run --rm runner pytest src/evaluate/tier2/test_live_leader_kill.py -v
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from src.evaluate import helpers as H

pytestmark = pytest.mark.live

SEED = 60003  # single-use per DB lifetime: a reused seed makes discover
              # skip every file and the job completes before the kill lands
OUT = Path("loadtest-results/tier2")


def test_live_leader_kill_end_to_end():
    cfg = H.capture_settings()
    bound = cfg["leader_lock_ttl"] + cfg["leader_heartbeat_interval"]

    old = H.get_leader()
    assert old and old.get("holder_id"), "no current leader; is the stack up?"

    containers = H.find_service_containers("worker")
    assert len(containers) >= 1, (
        f"expected at least 1 worker container, found {len(containers)}; "
        "was the stack reconciled? run with --no-deps"
    )

    tenant = H.ensure_tenant("tier2")
    job = H.submit_report(
        tenant_id=tenant["id"], report_type="daily_import",
        payload={"n_files": 10, "rows_per_file": 2000,
                 "seed": SEED, "inject_errors": 0},
    )
    job_id = job["job_id"]
    H.wait_for_state(job_id, "RUNNING", timeout=60)

    container = H.holder_to_container(old["holder_id"])
    t_kill = H.kill_named(container)

    new = H.wait_for_leader_change(old["holder_id"], timeout=bound + 15)
    t_acquired = time.time()

    H.wait_for_state(job_id, "COMPLETED", timeout=300)

    events = [e["event"] for e in H.get_events(job_id)]
    result = {
        **H.result_metadata(),
        "analytic_failover_bound_s": round(bound, 2),
        "observed_kill_to_acquired_s": round(t_acquired - t_kill, 2),
        "killed_holder": old["holder_id"],
        "new_holder": new["holder_id"],
        "scheduled_event_count": events.count("SCHEDULED"),
        "events": events,
    }

    # the in-flight job completes on its worker regardless of who holds the
    # lease; only a fresh dispatch proves the new leader actually coordinates
    probe = H._api("POST", "/jobs", json={
        "tenant_id": tenant["id"], "name": "post-failover-dispatch-probe",
        "job_type": "src.reporting.jobs.sleep_job:SleepJob",
        "payload": {"duration_s": 2.0},
        "max_retries": 3, "timeout_seconds": 120,
    })
    H.wait_for_state(probe["id"], "COMPLETED", timeout=90)
    result["post_failover_dispatch"] = "ok"

    OUT.mkdir(parents=True, exist_ok=True)
    H.write_json(OUT / f"leader_kill_{H.ts_now()}.json", result)

    assert result["new_holder"] != result["killed_holder"]
    assert result["observed_kill_to_acquired_s"] <= bound + 5.0
    assert result["scheduled_event_count"] == 1, "duplicate dispatch detected"
