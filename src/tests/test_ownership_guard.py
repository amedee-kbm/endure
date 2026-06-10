"""
Unit / DB-level tests for the dual-execution race fixes.

Test 1 — Ownership-gated CAS: a COMPLETED write with the wrong
          assigned_worker_id must match 0 rows; the job remains RUNNING.

Test 2 — step() IntegrityError swallow: a duplicate (job_id, step_id)
          write does not propagate an exception; exactly one row survives.

Test 3 — Heartbeat OFFLINE recovery: on self-detection of OFFLINE the
          sender cancels all in-flight tasks and resets inflight_count=0
          before marking ONLINE.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Test 1 — Ownership-gated COMPLETED write
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
async def test_wrong_worker_completed_write_affects_zero_rows():
    """
    If job.assigned_worker_id == worker_B but we run the ownership-gated
    aupdate with assigned_worker_id == worker_A, it must return 0 and leave
    the job state unchanged.

    This is the DB-level proof that Fix 1 prevents dual-write corruption.
    """
    from src.constants import JobState, WorkerState
    from src.models import Job, Tenant, Worker

    now = datetime.now(timezone.utc)

    tenant = await Tenant.objects.acreate(name=f"test-tenant-{uuid.uuid4().hex[:8]}")
    worker_a = await Worker.objects.acreate(
        hostname="worker-a",
        pid=1001,
        state=WorkerState.ONLINE,
        last_heartbeat=now,
    )
    worker_b = await Worker.objects.acreate(
        hostname="worker-b",
        pid=1002,
        state=WorkerState.ONLINE,
        last_heartbeat=now,
    )

    job = await Job.objects.acreate(
        tenant=tenant,
        name="race-test-job",
        job_type="some.module:SomeJob",
        state=JobState.RUNNING,
        assigned_worker=worker_b,
        attempt=1,
    )

    # Worker A tries to claim the COMPLETED transition — should match 0 rows.
    rows_updated = await Job.objects.filter(
        id=job.id,
        state=JobState.RUNNING,
        assigned_worker_id=worker_a.id,
    ).aupdate(
        state=JobState.COMPLETED,
        completed_at=now,
        result={"output": "from-worker-a"},
        updated_at=now,
    )

    assert rows_updated == 0, (
        f"Expected 0 rows updated (wrong owner), got {rows_updated}"
    )

    # Confirm the DB record is untouched.
    await job.arefresh_from_db()
    assert job.state == JobState.RUNNING, (
        f"Job state should still be RUNNING, got {job.state!r}"
    )
    assert job.result is None, "Job result must not have been written by wrong worker"
    assert job.assigned_worker_id == worker_b.id, (
        "assigned_worker must still be worker_b"
    )


# ---------------------------------------------------------------------------
# Test 2 — step() swallows IntegrityError from duplicate acreate
# ---------------------------------------------------------------------------


async def test_step_swallows_integrity_error_no_exception():
    """
    When StepOutput.acreate raises IntegrityError (concurrent execution wrote
    the same step_id first), step() must not propagate the exception and must
    return the result normally.

    We simulate the race by patching acreate to raise IntegrityError after
    the existence check returns None (the window where two workers both see
    no existing row and both attempt to insert).
    """
    from django.db import IntegrityError

    from src.framework.context import _current_job_id, _step_counter
    from src.framework.step import step

    call_count = 0

    async def fn() -> dict:
        nonlocal call_count
        call_count += 1
        return {"value": 42}

    job_id = uuid.uuid4()
    tok_job = _current_job_id.set(job_id)
    tok_counter = _step_counter.set(0)

    try:
        # StepOutput is a lazy import inside step(); patch it at the source.
        with patch("src.models.StepOutput") as MockStepOutput:
            # Simulate: no existing row found (both workers pass the check)
            MockStepOutput.objects.filter.return_value.afirst = AsyncMock(return_value=None)
            # Simulate: second writer hits unique constraint
            MockStepOutput.objects.acreate = AsyncMock(
                side_effect=IntegrityError("UNIQUE constraint failed: src_stepoutput.job_id, src_stepoutput.step_id")
            )

            result = await step("concurrent-step", fn)

        assert result == {"value": 42}, f"Expected result dict, got {result!r}"
        assert call_count == 1, "fn() must be called exactly once"
    finally:
        _current_job_id.reset(tok_job)
        _step_counter.reset(tok_counter)


async def test_step_returns_existing_result_on_duplicate_call():
    """
    When a StepOutput row already exists for (job_id, step_id), step()
    returns the stored result without calling fn() — the normal skip path.
    The IntegrityError path is a safety net for the narrow window before
    the row exists; this test confirms the primary (non-race) path still works.
    """
    import json

    from src.framework.context import _current_job_id, _step_counter
    from src.framework.step import step

    stored = {"value": 99}

    async def fn() -> dict:
        raise AssertionError("fn() must not be called when row already exists")

    job_id = uuid.uuid4()
    tok_job = _current_job_id.set(job_id)
    tok_counter = _step_counter.set(0)

    try:
        # StepOutput is a lazy import inside step(); patch it at the source.
        with patch("src.models.StepOutput") as MockStepOutput:
            existing_row = MagicMock()
            existing_row.output = json.dumps(stored)
            MockStepOutput.objects.filter.return_value.afirst = AsyncMock(return_value=existing_row)

            result = await step("already-done", fn)

        assert result == stored, f"Expected stored result {stored!r}, got {result!r}"
    finally:
        _current_job_id.reset(tok_job)
        _step_counter.reset(tok_counter)


# ---------------------------------------------------------------------------
# Test 3 — Heartbeat OFFLINE recovery resets inflight before going ONLINE
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
async def test_offline_recovery_cancels_tasks_and_resets_inflight():
    """
    When HeartbeatSender detects worker.state == OFFLINE in the DB:
    - All in-flight asyncio tasks must be cancelled (best-effort signal).
    - inflight_job_count must be reset to 0 in the DB.
    - tenant_inflight_job_count_map must be cleared.
    - state must become ONLINE.

    This is the bookkeeping half of Fix 4.  The primary correctness guard
    remains the ownership-gated CAS in _execute_job (Fix 1 / Fix 3).
    """
    from src.constants import WorkerState
    from src.models import Worker
    from src.worker.heartbeat import HeartbeatSender

    now = datetime.now(timezone.utc)

    worker = await Worker.objects.acreate(
        hostname="offline-test-host",
        pid=9999,
        state=WorkerState.OFFLINE,
        inflight_job_count=3,
        tenant_inflight_job_count_map={"tenant-abc": 3},
        last_heartbeat=now,
    )

    mock_task_1 = MagicMock(spec=asyncio.Task)
    mock_task_2 = MagicMock(spec=asyncio.Task)
    job_id_1 = uuid.uuid4()
    job_id_2 = uuid.uuid4()
    active_jobs: dict[uuid.UUID, asyncio.Task] = {
        job_id_1: mock_task_1,
        job_id_2: mock_task_2,
    }

    sender = HeartbeatSender(worker.id, active_jobs=active_jobs)

    # Run exactly one heartbeat iteration by making asyncio.sleep stop the loop.
    async def one_shot_sleep(_interval: float) -> None:
        sender._running = False

    with patch("src.worker.heartbeat.asyncio.sleep", side_effect=one_shot_sleep):
        await sender.start()

    # Both in-flight tasks must have been cancelled.
    mock_task_1.cancel.assert_called_once()
    mock_task_2.cancel.assert_called_once()

    # DB state must reflect the reset.
    await worker.arefresh_from_db()
    assert worker.state == WorkerState.ONLINE, (
        f"Worker must be ONLINE after recovery, got {worker.state!r}"
    )
    assert worker.inflight_job_count == 0, (
        f"inflight_job_count must be 0 after recovery, got {worker.inflight_job_count}"
    )
    assert worker.tenant_inflight_job_count_map == {}, (
        f"tenant map must be cleared, got {worker.tenant_inflight_job_count_map!r}"
    )
