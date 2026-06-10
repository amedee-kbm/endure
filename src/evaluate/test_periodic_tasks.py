"""
NEW (endure-specific): PeriodicTask scheduling — the scheduler automatically
spawns jobs for active periodic tasks when their cron expression fires.

This test creates a periodic task via the Django ORM (requires DB access)
or via a management API if one exists. Because periodic tasks need the
scheduler loop to tick, a running stack is required.

The cron expression "* * * * *" fires every minute; the test waits up to
90 s for the spawned job to appear and complete.
"""

import asyncio
import time

import pytest

from src.evaluate.helpers import (
    SYNTHETIC_JOB,
    create_tenant,
    get_job,
    unique_name,
    wait_for_job,
)

pytestmark = [pytest.mark.e2e, pytest.mark.evaluate, pytest.mark.asyncio]

# The periodic task API endpoint — extend admin router in future;
# currently the test seeds directly via Django shell / DB.
# Skip by default until a periodic-task creation API is added.
PERIODIC_TASK_API = "/api/v1/admin/periodic-tasks"


async def _create_periodic_task_via_api(client, *, name, tenant_id) -> str | None:
    """Try POST /api/v1/admin/periodic-tasks; return task id or None if endpoint missing."""
    r = await client.post(
        PERIODIC_TASK_API,
        json={
            "name": name,
            "job_type": SYNTHETIC_JOB,
            "cron_expression": "* * * * *",
            "tenant_id": tenant_id,
            "payload": {"stage_duration": 0.1, "stages": 2},
            "is_active": True,
        },
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("id")


async def _find_job_for_task(client, task_id: str, *, timeout: float = 90.0) -> str | None:
    """Poll GET /api/v1/jobs until a job spawned by the periodic task appears."""
    deadline = time.monotonic() + timeout
    seen: set[str] = set()
    while time.monotonic() < deadline:
        r = await client.get("/api/v1/jobs", params={"limit": 50})
        r.raise_for_status()
        for job in r.json().get("jobs", []):
            jid = job["id"]
            if jid not in seen and job.get("periodic_task_id") == task_id:
                return jid
            seen.add(jid)
        await asyncio.sleep(5.0)
    return None


async def test_periodic_task_spawns_and_completes_job(client):
    tenant_id = await create_tenant(client, name=unique_name("periodic"))
    task_name = unique_name("cron-task")

    task_id = await _create_periodic_task_via_api(client, name=task_name, tenant_id=tenant_id)
    if task_id is None:
        pytest.skip(
            "POST /api/v1/admin/periodic-tasks not yet implemented — "
            "seed the PeriodicTask row manually and re-run"
        )

    job_id = await _find_job_for_task(client, task_id, timeout=90.0)
    assert job_id is not None, (
        f"No job spawned for periodic task {task_id!r} within 90 s"
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] == "COMPLETED"
