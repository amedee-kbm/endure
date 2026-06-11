"""
Tier 1 — worker-capacity gate pins dispatch to the source of truth.

The gate must derive a worker's load from live job rows (assigned jobs in
SCHEDULED/RUNNING), never from a maintained counter. The counter variant had
three uncoordinated read-modify-write writers (scheduler assign, worker
completion, heartbeat) whose lost updates ratcheted the counter to the cap
and permanently starved dispatch with idle workers (E4b w=2, 2026-06-11).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from src.constants import JobState, WorkerState
from src.models import Worker
from src.scheduler.scheduler import Scheduler

pytestmark = pytest.mark.asyncio


async def make_worker(max_inflight: int) -> Worker:
    return await Worker.objects.acreate(
        id=uuid.uuid4(),
        hostname=f"cap-{uuid.uuid4().hex[:8]}",
        pid=1,
        max_inflight_jobs=max_inflight,
        state=WorkerState.ONLINE,
        last_heartbeat=datetime.now(timezone.utc),
    )


async def test_gate_blocks_at_live_capacity(make_job):
    w = await make_worker(max_inflight=2)
    await make_job(state=JobState.RUNNING, assigned_worker=w)
    await make_job(state=JobState.RUNNING, assigned_worker=w)

    assert await Scheduler()._find_available_worker() is None


async def test_gate_frees_when_job_completes(make_job):
    w = await make_worker(max_inflight=2)
    await make_job(state=JobState.RUNNING, assigned_worker=w)
    j = await make_job(state=JobState.RUNNING, assigned_worker=w)

    assert await Scheduler()._find_available_worker() is None

    j.state = JobState.COMPLETED
    await j.asave(update_fields=["state"])

    found = await Scheduler()._find_available_worker()
    assert found is not None and found.id == w.id


async def test_least_loaded_worker_wins(make_job):
    busy = await make_worker(max_inflight=4)
    idle = await make_worker(max_inflight=4)
    await make_job(state=JobState.RUNNING, assigned_worker=busy)
    await make_job(state=JobState.SCHEDULED, assigned_worker=busy)

    found = await Scheduler()._find_available_worker()
    assert found is not None and found.id == idle.id
