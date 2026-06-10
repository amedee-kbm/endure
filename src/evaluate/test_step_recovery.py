"""
Step-level recovery test — §4.3 Scenario 2.

Submits a DailyImportJob with n_files=10, waits until at least 3 files have
been ingested (step outputs recorded), kills the assigned worker container,
then asserts that the job completes with all 10 step outputs recorded exactly
once (no step re-executed).

Requires a running Docker Compose stack and ENDURE_CHAOS=1.
"""

import asyncio
import subprocess

import pytest

from src.evaluate.helpers import (
    create_tenant,
    get_assigned_worker_hostname,
    unique_name,
    wait_for_job,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.evaluate,
    pytest.mark.chaos,
    pytest.mark.asyncio,
]

DAILY_IMPORT = "src.reporting.jobs.daily_import:DailyImportJob"


async def _wait_for_step_outputs(client, job_id: str, *, min_count: int, timeout: float = 60.0) -> dict:
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = await client.get(f"/api/v1/jobs/{job_id}/step-outputs")
        r.raise_for_status()
        data = r.json()
        if data["count"] >= min_count:
            return data
        await asyncio.sleep(0.5)
    raise TimeoutError(
        f"Job {job_id} did not reach {min_count} step output(s) within {timeout}s"
    )


async def test_step_level_recovery_after_worker_crash(client):
    """Worker crash mid-ingest resumes from the next unprocessed file."""
    n_files = 10
    tenant_id = await create_tenant(client, name=unique_name("rpt-step-recovery"))

    r = await client.post(
        "/api/v1/reports",
        json={
            "tenant_id": tenant_id,
            "report_type": "daily_import",
            "payload": {
                "date": "2026-06-04",
                "n_files": n_files,
                "rows_per_file": 200,
                "seed": 42,
                "inject_errors": 2,
            },
            "max_retries": 3,
            "timeout_seconds": 300,
        },
    )
    r.raise_for_status()
    job_id = str(r.json()["job_id"])

    # Wait for at least 3 files ingested (3 step outputs)
    await _wait_for_step_outputs(client, job_id, min_count=3, timeout=60.0)

    # Identify and kill the assigned worker
    hostname = await get_assigned_worker_hostname(client, job_id, timeout=30.0)
    subprocess.run(["docker", "kill", hostname], check=False, capture_output=True)

    # Wait for coordinator to re-queue and a new worker to complete the job
    job = await wait_for_job(client, job_id, timeout=180.0)
    assert job["state"] == "COMPLETED", f"Expected COMPLETED after recovery, got {job['state']}"

    # All 10 files should be recorded exactly once
    r2 = await client.get(f"/api/v1/jobs/{job_id}/step-outputs")
    r2.raise_for_status()
    step_data = r2.json()
    assert step_data["count"] == n_files, (
        f"Expected {n_files} step outputs (one per file), got {step_data['count']}"
    )
