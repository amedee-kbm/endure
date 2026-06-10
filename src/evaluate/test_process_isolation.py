"""
NEW (endure-specific): subprocess process isolation — jobs run in a separate
process with resource limits. Enabled via ENDURE_USE_PROCESS_ISOLATION=true.

These tests run against a stack started with ENDURE_USE_PROCESS_ISOLATION=1
in the worker containers, or are skipped otherwise.

The docker-compose.evaluate.yml can be extended with an override that sets
  ENDURE_USE_PROCESS_ISOLATION: "true"
on the worker services to enable isolated execution for this test suite.
"""

import os

import pytest

from src.evaluate.helpers import (
    SYNTHETIC_JOB,
    create_tenant,
    get_events,
    submit_job,
    unique_name,
    wait_for_job,
)

pytestmark = [pytest.mark.e2e, pytest.mark.evaluate, pytest.mark.asyncio]


def _isolation_enabled() -> bool:
    return os.environ.get("ENDURE_USE_PROCESS_ISOLATION", "").strip() in (
        "1",
        "true",
        "yes",
    )


@pytest.fixture(autouse=True)
def require_isolation():
    if not _isolation_enabled():
        pytest.skip(
            "Set ENDURE_USE_PROCESS_ISOLATION=1 to run process-isolation evaluation tests"
        )


async def test_isolated_job_completes(client):
    tenant_id = await create_tenant(client, name=unique_name("iso-ok"))
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        job_type=SYNTHETIC_JOB,
        payload={"stage_duration": 0.2, "stages": 3},
    )
    job = await wait_for_job(client, job_id, timeout=30.0)
    assert job["state"] == "COMPLETED"


async def test_isolated_failing_job_records_error(client):
    tenant_id = await create_tenant(client, name=unique_name("iso-fail"))
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        job_type=SYNTHETIC_JOB,
        payload={"stage_duration": 0.1, "fail_at_stage": 0},
        max_retries=0,
    )
    job = await wait_for_job(client, job_id, timeout=20.0)
    assert job["state"] in {"FAILED", "DEAD_LETTER"}
    assert job.get("error_message") or any(
        e.get("detail") for e in await get_events(client, job_id) if e["event"] == "FAILED"
    ), "Expected an error detail on the FAILED event"


async def test_isolated_checkpointing_job_persists_checkpoint(client):
    tenant_id = await create_tenant(client, name=unique_name("iso-ckpt"))
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        job_type=SYNTHETIC_JOB,
        payload={"stage_duration": 0.2, "stages": 5},
    )
    job = await wait_for_job(client, job_id, timeout=45.0)
    assert job["state"] == "COMPLETED"

    r = await client.get(f"/api/v1/jobs/{job_id}/checkpoints")
    r.raise_for_status()
    data = r.json()
    assert int(data.get("total", 0)) >= 1, "Expected at least one checkpoint record"
