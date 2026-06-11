"""Ghost-run arbitration at the database boundary (§3 ghost runs)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest
from django.db import IntegrityError

from src.checkpoint.manager import checkpoint_manager
from src.constants import JobState, WorkerState
from src.framework.context import _current_job_id, _current_stage, _step_counter
from src.framework.step import step
from src.models import Checkpoint, Job, StepOutput, Worker

pytestmark = pytest.mark.asyncio


async def _make_worker():
    return await Worker.objects.acreate(
        id=uuid.uuid4(),
        hostname="tier1",
        pid=1,
        max_inflight_jobs=4,
        state=WorkerState.ONLINE,
        last_heartbeat=datetime.now(timezone.utc),
    )


async def test_ghost_completed_write_matches_zero_rows(make_job):
    owner, ghost = await _make_worker(), await _make_worker()
    job = await make_job(state=JobState.RUNNING, assigned_worker=owner)

    updated = await Job.objects.filter(
        id=job.id, state=JobState.RUNNING, assigned_worker_id=ghost.id
    ).aupdate(state=JobState.COMPLETED, result={"by": "ghost"})

    assert updated == 0
    await job.arefresh_from_db()
    assert job.state == JobState.RUNNING and job.result is None


async def test_stale_assignment_cas_misses_on_cancelled(make_job):
    job = await make_job(state=JobState.CANCELLED, assigned_worker=None)
    w = await _make_worker()
    updated = await Job.objects.filter(id=job.id, state=JobState.QUEUED).aupdate(
        state=JobState.SCHEDULED, assigned_worker_id=w.id
    )
    assert updated == 0


async def test_duplicate_step_row_rejected_and_replayed(make_job):
    job = await make_job()
    await StepOutput.objects.acreate(
        job_id=job.id, stage_name="ingest", step_id=0, step_name="x",
        output=json.dumps("first"),
    )
    # The constraint is real:
    with pytest.raises(IntegrityError):
        await StepOutput.objects.acreate(
            job_id=job.id, stage_name="ingest", step_id=0, step_name="x",
            output=json.dumps("second"),
        )
    # And step() replays the stored row instead of executing:
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        return "second"

    t1 = _current_job_id.set(job.id)
    t2 = _current_stage.set("ingest")
    t3 = _step_counter.set(0)
    try:
        result = await step("x", fn)
    finally:
        _current_job_id.reset(t1)
        _current_stage.reset(t2)
        _step_counter.reset(t3)
    assert result == "first" and calls == 0


async def test_duplicate_checkpoint_sequence_swallowed(make_job):
    job = await make_job()
    first = await checkpoint_manager.save_checkpoint(job.id, 1, b'{"a": 1}')
    second = await checkpoint_manager.save_checkpoint(job.id, 1, b'{"a": 1}')
    assert first is not None and second is None
    assert await Checkpoint.objects.filter(job_id=job.id).acount() == 1
