"""
Evaluation test: step-level fault recovery mid-ingest.
Submits DailyImportJob, waits for several step-outputs, kills the worker,
then asserts the job resumes and completes without repeating ingested files.

Requires running docker stack with >= 2 workers:
  docker-compose up -d --scale worker=2
"""
import asyncio
import os
import subprocess
import time

import pytest

from src.evaluate.helpers import (
    create_tenant,
    submit_job,
    wait_for_job,
    get_assigned_worker_hostname,
    unique_name,
    TERMINAL_STATES,
)

pytestmark = [pytest.mark.evaluate, pytest.mark.e2e, pytest.mark.chaos]

JOB_TYPE = "src.reporting.jobs.daily_import:DailyImportJob"
N_FILES = 10
ROWS_PER_FILE = 200
MIN_STEPS_BEFORE_KILL = 3


def _chaos_enabled() -> bool:
    return os.environ.get("ENDURE_CHAOS", "").strip() in ("1", "true", "yes")


@pytest.mark.asyncio
async def test_step_recovery_after_worker_kill(client, api_url, require_stack):
    """
    DailyImportJob with 10 files: kill worker after 3+ step-outputs,
    verify job resumes and completes all 10 files without re-ingesting.
    """
    if not _chaos_enabled():
        pytest.skip("Set ENDURE_CHAOS=1 to run container fault-injection tests")

    tenant_id = await create_tenant(client, name=unique_name("step-recovery"))

    # Submit job directly (not via reports API — needs max_retries + timeout control)
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        name=unique_name("step-recovery"),
        job_type=JOB_TYPE,
        payload={
            "tenant_id": tenant_id,
            "n_files": N_FILES,
            "rows_per_file": ROWS_PER_FILE,
            "seed": 99,
            "inject_errors": 0,
        },
        max_retries=3,
        timeout_seconds=300,
    )

    # Poll step-outputs until at least MIN_STEPS_BEFORE_KILL completed
    deadline = time.monotonic() + 120
    step_count = 0
    while time.monotonic() < deadline:
        resp = await client.get(f"{api_url}/api/v1/jobs/{job_id}/step-outputs")
        if resp.status_code == 200:
            step_count = resp.json().get("count", 0)
            if step_count >= MIN_STEPS_BEFORE_KILL:
                break
        await asyncio.sleep(2)

    assert step_count >= MIN_STEPS_BEFORE_KILL, (
        f"Only {step_count} steps completed before timeout"
    )

    # Get the assigned worker's hostname to kill it
    hostname = await get_assigned_worker_hostname(client, job_id, timeout=30)
    assert hostname, "Could not determine assigned worker hostname"

    # Kill the worker container by hostname (don't assert — may already be gone)
    subprocess.run(
        ["docker", "kill", hostname],
        capture_output=True,
        text=True,
    )

    # Wait for job to eventually complete (coordinator re-queues, new worker picks up)
    job = await wait_for_job(client, job_id, target_states=TERMINAL_STATES, timeout=300)
    assert job["state"] == "COMPLETED", (
        f"Expected COMPLETED after recovery, got: {job['state']}: {job.get('error_message')}"
    )

    # Verify all 10 step-outputs present (no repeats — step() is idempotent)
    step_resp = await client.get(f"{api_url}/api/v1/jobs/{job_id}/step-outputs")
    assert step_resp.status_code == 200
    final_step_count = step_resp.json().get("count", 0)
    assert final_step_count == N_FILES, (
        f"Expected {N_FILES} step outputs, got {final_step_count}"
    )

    # Verify artifact produced
    result = job.get("result", {})
    assert result and result.get("artifact_path", "").endswith(".xlsx"), (
        f"Missing .xlsx artifact in result: {result}"
    )
