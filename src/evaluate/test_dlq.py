"""RQ2: jobs that exhaust retries land in the dead-letter queue."""

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

TERMINAL_WITH_DLQ = frozenset({"COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED", "TIMED_OUT"})


async def test_failing_job_enters_dead_letter(client):
    tenant_id = await create_tenant(client, name=unique_name("dlq"))
    # fail_at_stage=0 means the very first stage raises; max_retries=2 → 3 total attempts
    job_id = await submit_job(
        client,
        tenant_id=tenant_id,
        job_type=SYNTHETIC_JOB,
        payload={"stage_duration": 0.1, "fail_at_stage": 0},
        max_retries=2,
    )

    job = await wait_for_job(client, job_id, target_states=TERMINAL_WITH_DLQ, timeout=30.0)
    assert job["state"] == "DEAD_LETTER"

    r = await client.get("/api/v1/admin/dead-letter")
    r.raise_for_status()
    dlq = r.json()
    match = next((d for d in dlq["items"] if d["job_id"] == job_id), None)
    assert match is not None, f"Job {job_id} not found in dead-letter queue"
    assert match["total_attempts"] >= 2

    events = await get_events(client, job_id)
    event_types = [e["event"] for e in events]
    assert "FAILED" in event_types
    assert "DEAD_LETTER" in event_types
