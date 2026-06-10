"""RQ2: checkpoint resume — interrupted jobs skip completed stages on restart.

Requires ENDURE_CHAOS=1 and a running Docker Compose stack.
Set ENDURE_WORKER_CONTAINER to override the default container target.
"""

from __future__ import annotations

import asyncio
import os
import subprocess

import pytest

from src.evaluate.helpers import (
    SYNTHETIC_JOB,
    create_tenant,
    get_assigned_worker_hostname,
    get_events,
    submit_job,
    unique_name,
    wait_for_checkpoint,
    wait_for_job,
    wait_for_running_count,
)

pytestmark = [pytest.mark.e2e, pytest.mark.chaos, pytest.mark.evaluate, pytest.mark.asyncio]


def _chaos_enabled() -> bool:
    return os.environ.get("ENDURE_CHAOS", "").strip() in ("1", "true", "yes")


def _docker_stop(container: str) -> None:
    subprocess.run(["docker", "stop", container], check=True, capture_output=True, timeout=30)


def _docker_start(container: str) -> None:
    subprocess.run(["docker", "start", container], check=True, capture_output=True, timeout=30)


async def test_checkpoint_resume_after_worker_kill(client):
    if not _chaos_enabled():
        pytest.skip("Set ENDURE_CHAOS=1 to run container fault-injection tests")

    tenant_id = await create_tenant(client, name=unique_name("ckpt"))
    # Checkpoints are saved at stage boundaries (stage_duration=2s per stage).
    # We wait for 2 checkpoints so at least 2 stages are recorded before we kill.
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        job_type=SYNTHETIC_JOB,
        payload={"stage_duration": 2.0, "stages": 5},
        max_retries=3,
        name=unique_name("ckpt-job"),
        timeout_seconds=60,
    )

    await wait_for_running_count(client, [job_id], min_running=1, timeout=10.0)
    await wait_for_checkpoint(client, job_id, min_total=2, timeout=10.0)

    # Prefer explicit ENDURE_WORKER_CONTAINER; fall back to API-resolved hostname
    container = os.environ.get("ENDURE_WORKER_CONTAINER", "").strip()
    target = container or await get_assigned_worker_hostname(client, job_id)

    _docker_stop(target)
    try:
        job_done = await wait_for_job(client, job_id, timeout=90.0)
        assert job_done["state"] == "COMPLETED", (
            f"Job ended in {job_done['state']!r} instead of COMPLETED"
        )
        # Verify all 5 stages completed (correct artifact — resumed job skipped
        # already-done stages and ran the remaining ones to produce a full result).
        result = job_done.get("result") or {}
        completed_stages = result.get("completed_stages", [])
        assert len(completed_stages) == 5, (
            f"Expected 5 completed stages in result, got {completed_stages}"
        )
        events = await get_events(client, job_id)
        combined = " ".join((e.get("detail") or "").lower() for e in events)
        assert "skip" in combined, (
            "Expected a 'skip' or 'skipping' detail in events after checkpoint resume"
        )
    finally:
        _docker_start(target)
        await asyncio.sleep(5)


