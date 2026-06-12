"""
Tier 1 — the unhandled-exception path must not strand jobs.

Before migration 0014, an unhandled crash inside ``WorkerNode._execute_job``
wrote a terminal ``FAILED`` state with no exit. These tests pin the new
contract: a crash is routed through the same retry/dead-letter logic as a
normal result-failure.

  (a) retries remaining   → re-queued with run_after, completable by a healthy
                            worker on re-dispatch.
  (b) retries exhausted   → DEAD_LETTER + DeadLetterJob row, resurrectable via
                            the manual-retry endpoint.

We drive ``_execute_job`` directly with a stub executor whose ``execute``
raises, which is exactly the failure mode the ``except Exception`` arm guards.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.api.routes.jobs import retry_job
from src.constants import JobState
from src.models import DeadLetterJob, Job, JobEvent, Worker
from src.worker.executor import JobExecutor
from src.worker.worker import WorkerNode

pytestmark = pytest.mark.asyncio

JOB_OK = "src.evaluate.tier1.jobs:GateJob"  # no gates set ⇒ runs to completion


class _BoomExecutor:
    """Executor stand-in that raises instead of returning a result dict."""

    async def execute(self, *args, **kwargs):
        raise RuntimeError("boom: simulated unhandled executor crash")


async def _register_worker() -> WorkerNode:
    node = WorkerNode(max_inflight_jobs=4)
    await Worker.objects.acreate(
        id=node.worker_id,
        hostname=node.hostname,
        pid=node.pid,
        max_inflight_jobs=node.max_inflight_jobs,
        state="ONLINE",
        last_heartbeat=datetime.now(timezone.utc),
    )
    return node


async def _events(job_id) -> list[str]:
    return [e async for e in JobEvent.objects.filter(job_id=job_id)
            .order_by("timestamp").values_list("event", flat=True)]


async def test_unhandled_exception_with_retries_requeues_and_completes(make_job):
    node = await _register_worker()
    node.executor = _BoomExecutor()  # type: ignore[assignment]

    job = await make_job(
        job_type=JOB_OK,
        payload={"disable_checkpointing": True},
        state=JobState.SCHEDULED,
        assigned_worker=await Worker.objects.aget(id=node.worker_id),
        attempt=0,
        max_retries=3,
    )

    # Crash inside execution. The except-arm must re-queue, not write FAILED.
    await node._execute_job(job.id)

    job = await Job.objects.aget(id=job.id)
    assert job.state == JobState.QUEUED, f"expected re-queue, got {job.state}"
    assert job.run_after is not None, "retry must set a backoff run_after"
    assert job.assigned_worker_id is None
    assert job.error_message == "Worker execution error"
    assert job.attempt == 1  # SCHEDULED→RUNNING incremented attempt once

    events = await _events(job.id)
    assert "FAILED" in events and "RETRIED" in events
    assert "DEAD_LETTER" not in events

    # A healthy worker re-dispatches the same job and drives it to COMPLETED.
    node.executor = JobExecutor()
    await Job.objects.filter(id=job.id).aupdate(
        state=JobState.SCHEDULED,
        assigned_worker_id=node.worker_id,
        started_at=None,
        run_after=None,
    )
    await node._execute_job(job.id)

    job = await Job.objects.aget(id=job.id)
    assert job.state == JobState.COMPLETED, f"healthy re-run should complete, got {job.state}"


async def test_unhandled_exception_exhausted_dead_letters_then_manual_retry(make_job):
    node = await _register_worker()
    node.executor = _BoomExecutor()  # type: ignore[assignment]

    # attempt=2, max_retries=3 ⇒ RUNNING bumps attempt to 3; 3 < 3 is False ⇒ DLQ.
    job = await make_job(
        job_type=JOB_OK,
        payload={"disable_checkpointing": True},
        state=JobState.SCHEDULED,
        assigned_worker=await Worker.objects.aget(id=node.worker_id),
        attempt=2,
        max_retries=3,
    )

    await node._execute_job(job.id)

    job = await Job.objects.aget(id=job.id)
    assert job.state == JobState.DEAD_LETTER, f"expected DLQ, got {job.state}"
    assert job.assigned_worker_id is None

    dlq = await DeadLetterJob.objects.aget(job_id=job.id)
    assert dlq.final_error == "Worker execution error"
    assert dlq.total_attempts == 3

    events = await _events(job.id)
    assert "FAILED" in events and "DEAD_LETTER" in events
    assert "RETRIED" not in events

    # Manual retry endpoint resurrects it: back to QUEUED, attempt reset, DLQ gone.
    resurrected = await retry_job(None, job.id)
    assert resurrected.state == JobState.QUEUED
    assert resurrected.attempt == 0
    assert not await DeadLetterJob.objects.filter(job_id=job.id).aexists()

    events = await _events(job.id)
    assert "MANUAL_RETRY" in events and events[-1] == "QUEUED"
