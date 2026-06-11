"""
Time as data: every time-gated decision in Endure compares now against a
stored timestamp, so tests write the past into rows instead of waiting.
Boundary cases (just-inside vs just-outside the timeout) are pinned exactly —
something wall-clock experiments can never do.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from django.conf import settings

from src.constants import JobState, WorkerState
from src.models import DeadLetterJob, Job, JobEvent, SchedulerLeader, Worker
from src.queue.pg_queue import pg_queue
from src.scheduler.leader import LEADER_SINGLETON_ID, LeaderElection
from src.scheduler.scheduler import Scheduler

pytestmark = pytest.mark.asyncio


def _ago(seconds: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


async def _worker(stale_by: float) -> Worker:
    return await Worker.objects.acreate(
        id=uuid.uuid4(), hostname="t", pid=1, max_inflight_jobs=4,
        state=WorkerState.ONLINE, last_heartbeat=_ago(stale_by),
    )


async def test_sweep_boundary(make_job):
    timeout = settings.WORKER_HEARTBEAT_TIMEOUT
    fresh = await _worker(stale_by=timeout - 0.5)
    stale = await _worker(stale_by=timeout + 0.5)
    job = await make_job(state=JobState.RUNNING, assigned_worker=stale)

    await Scheduler()._detect_dead_workers()

    await fresh.arefresh_from_db()
    await stale.arefresh_from_db()
    await job.arefresh_from_db()
    assert fresh.state == WorkerState.ONLINE, "false positive inside the bound"
    assert stale.state == WorkerState.OFFLINE, "missed detection outside the bound"
    # Orphan re-dispatched: back to QUEUED, ownership cleared, retry deferred.
    assert job.state == JobState.QUEUED
    assert job.assigned_worker_id is None
    assert job.run_after is not None and job.run_after > datetime.now(timezone.utc)
    assert await JobEvent.objects.filter(job_id=job.id, event="RETRIED").aexists()


async def test_lease_takeover_on_expiry_only():
    ttl = settings.LEADER_LOCK_TTL
    now = datetime.now(timezone.utc)
    await SchedulerLeader.objects.acreate(
        id=LEADER_SINGLETON_ID, holder_id="A",
        acquired_at=now, renewed_at=_ago(ttl + 1),
    )
    b = LeaderElection("B")
    assert await b.try_acquire() is True  # expired → takeover

    c = LeaderElection("C")
    assert await c.try_acquire() is False  # fresh lease → denied

    a = LeaderElection("A")
    a.is_leader = True  # stale self-belief, as after supersession
    assert await a.renew_heartbeat() is False and a.is_leader is False

    row = await SchedulerLeader.objects.aget(id=LEADER_SINGLETON_ID)
    assert row.holder_id == "B"


async def test_run_after_gates_dispatch(make_job):
    deferred = await make_job(
        state=JobState.QUEUED, assigned_worker=None,
        run_after=datetime.now(timezone.utc) + timedelta(seconds=60),
    )
    assert await pg_queue.dequeue_job() is None

    await Job.objects.filter(id=deferred.id).aupdate(run_after=_ago(1))
    assert await pg_queue.dequeue_job() == str(deferred.id)


async def test_timeout_sweep_requeues(make_job):
    w = await _worker(stale_by=0)
    job = await make_job(
        state=JobState.RUNNING, assigned_worker=w,
        timeout_seconds=300, started_at=_ago(301), attempt=1,
    )
    await Scheduler()._detect_timed_out_jobs()
    await job.arefresh_from_db()
    assert job.state == JobState.QUEUED
    assert await JobEvent.objects.filter(job_id=job.id, event="TIMED_OUT").aexists()
    assert await JobEvent.objects.filter(job_id=job.id, event="RETRIED").aexists()


async def test_exhausted_attempts_dead_letter(make_job):
    w = await _worker(stale_by=0)
    job = await make_job(state=JobState.RUNNING, assigned_worker=w,
                         attempt=3, max_retries=3)
    await Scheduler()._handle_job_failure(job, error="boom")
    await job.arefresh_from_db()
    assert job.state == JobState.DEAD_LETTER
    assert await DeadLetterJob.objects.filter(job_id=job.id).aexists()
