"""RQ1/RQ2 smoke: submit a synthetic job and verify completion + audit trail."""

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


async def test_synthetic_job_completes(client):
    tenant_id = await create_tenant(client, name=unique_name("lifecycle"))
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        job_type=SYNTHETIC_JOB,
        payload={"stage_duration": 0.3, "stages": 5},
    )
    job = await wait_for_job(client, job_id, timeout=45.0)
    assert job["state"] == "COMPLETED"

    events = await get_events(client, job_id)
    event_types = [e["event"] for e in events]
    assert "QUEUED" in event_types
    assert "RUNNING" in event_types
    assert "COMPLETED" in event_types
