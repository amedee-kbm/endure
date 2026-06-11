"""
Tier 3 / E4a — pure scheduler-scaling sweep with SleepJob.

Work per job is an incompressible 10 s sleep, so deviation from ideal scaling
is scheduler overhead by construction. No SourceFile state is created, so no
volume wipes are needed between configurations — restart workers at the new
scale and rerun.

Per worker count N (set ENDURE_E4_WORKERS):
  dc up -d --scale worker=N --wait
  dc run --rm -e ENDURE_E4_WORKERS=N runner \\
      pytest src/evaluate/tier3/test_sleep_sweep.py -v

Outputs: loadtest-results/e4a/sleep_wN_<ts>.csv   (per-job rows + makespan)
         loadtest-results/e4a/drain_wN_<ts>.csv   (queue-depth time series)
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from src.evaluate import helpers as H
from src.evaluate.helpers import DrainSampler

pytestmark = pytest.mark.live

N_JOBS = 20
DURATION_S = 10.0
SLEEP_JOB_TYPE = "src.reporting.jobs.sleep_job:SleepJob"
OUT = Path("loadtest-results/e4a")


def _submit_sleep_job(tenant_id: str, name: str) -> dict:
    return H._api(
        "POST", "/jobs",
        json={
            "tenant_id": tenant_id, "name": name,
            "job_type": SLEEP_JOB_TYPE,
            "payload": {"duration_s": DURATION_S},
            "max_retries": 3, "timeout_seconds": 600,
        },
    )


def test_sleep_sweep_at_configured_worker_count():
    workers = int(os.environ["ENDURE_E4_WORKERS"])
    online = H.get_workers(state="ONLINE")
    assert len(online) == workers, (
        f"expected {workers} ONLINE workers, found {len(online)}; "
        f"restart the stack with --scale worker={workers}"
    )

    tenant = H.ensure_tenant("tier3-sleep")
    ts = H.ts_now()

    with DrainSampler() as sampler:
        t_submit = time.time()
        job_ids = [
            _submit_sleep_job(tenant["id"], f"sleep-{workers}w-{i}")["id"]
            for i in range(N_JOBS)
        ]
        rows = []
        for jid in job_ids:
            H.wait_for_state(jid, "COMPLETED", timeout=N_JOBS * DURATION_S + 120)
        t_done = time.time()
        for jid in job_ids:
            j = H.get_job(jid)
            dur = H.epoch(j["completed_at"]) - H.epoch(j["started_at"])
            rows.append({
                "worker_count": workers, "job_id": jid,
                "state": j["state"], "duration_s": round(dur, 2),
            })

    makespan = round(t_done - t_submit, 2)
    ideal = N_JOBS * DURATION_S / workers
    for r in rows:
        r["makespan_s"] = makespan
        r["ideal_makespan_s"] = round(ideal, 2)

    OUT.mkdir(parents=True, exist_ok=True)
    H.write_csv(OUT / f"sleep_w{workers}_{ts}.csv", rows)
    H.write_csv(OUT / f"drain_w{workers}_{ts}.csv", sampler.rows)

    assert all(r["state"] == "COMPLETED" for r in rows)
    # Generous validity ceiling; analyze.py computes the real efficiency.
    assert makespan < ideal * 2 + 60
