"""
Worker main loop.
Polls PostgreSQL for job assignments, executes them, reports results.
"""

import asyncio
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

from django.conf import settings
from django.db.models import F

from src.constants import JobState, WorkerState
from src.models import DeadLetterJob, Job, Worker
from src.scheduler.priority_queue import compute_retry_delay
from src.services.event_logger import record_event
from src.worker.executor import JobExecutor
from src.worker.heartbeat import HeartbeatSender

logger = logging.getLogger("endure.worker")


class WorkerNode:
    def __init__(self, max_inflight_jobs: int | None = None):
        self.worker_id = uuid.uuid4()
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.max_inflight_jobs = max_inflight_jobs or settings.WORKER_MAX_INFLIGHT_JOBS
        self.executor = JobExecutor()
        self._running = False
        self._active_jobs: dict[uuid.UUID, asyncio.Task] = {}
        self._cancelled_jobs: set[uuid.UUID] = set()
        # Pass the dict by reference — HeartbeatSender sees live task insertions.
        self.heartbeat = HeartbeatSender(self.worker_id, active_jobs=self._active_jobs)

    async def start(self):
        """Register worker and start processing loop."""
        logger.info(
            f"Worker {self.worker_id} starting on {self.hostname}:{self.pid}..."
        )

        await Worker.objects.acreate(
            id=self.worker_id,
            hostname=self.hostname,
            pid=self.pid,
            max_inflight_jobs=self.max_inflight_jobs,
            state=WorkerState.ONLINE,
            last_heartbeat=datetime.now(timezone.utc),
        )

        self._running = True

        await asyncio.gather(
            self.heartbeat.start(),
            self._poll_for_assigned_jobs(),
        )

    async def stop(self):
        """Gracefully shut down the worker."""
        self._running = False
        self.heartbeat.stop()

        for job_id, task in self._active_jobs.items():
            task.cancel()
            logger.info(f"Cancelled active job {job_id}")

        worker = await Worker.objects.filter(id=self.worker_id).afirst()
        if worker:
            worker.state = WorkerState.OFFLINE
            await worker.asave(update_fields=["state"])

        logger.info(f"Worker {self.worker_id} stopped.")

    async def _poll_for_assigned_jobs(self):
        """Poll DB for newly assigned jobs and detect cancelled/stolen ones."""
        poll_interval = getattr(settings, "WORKER_POLL_INTERVAL", 1.0)
        while self._running:
            try:
                # Pick up newly SCHEDULED jobs assigned to this worker.
                jobs = [
                    j
                    async for j in Job.objects.filter(
                        assigned_worker_id=self.worker_id, state=JobState.SCHEDULED
                    )
                ]
                for job in jobs:
                    if job.id not in self._active_jobs:
                        task = asyncio.create_task(self._execute_job(job.id))
                        self._active_jobs[job.id] = task

                # Cancel local tasks for jobs that lost ownership in the DB
                # (cancelled by API, or re-dispatched after false-suspect eviction).
                if self._active_jobs:
                    active_ids = list(self._active_jobs.keys())
                    valid_ids: set[uuid.UUID] = set()
                    async for jid in Job.objects.filter(
                        id__in=active_ids,
                        state__in=[JobState.SCHEDULED, JobState.RUNNING],
                        assigned_worker_id=self.worker_id,
                    ).values_list("id", flat=True):
                        valid_ids.add(jid)
                    for job_id in active_ids:
                        if job_id not in valid_ids and job_id not in self._cancelled_jobs:
                            self._cancel_job(job_id)

            except Exception:
                logger.exception("Error in job polling")

            await asyncio.sleep(poll_interval)

    def _cancel_job(self, job_id: uuid.UUID):
        """Cancel a running job task."""
        self._cancelled_jobs.add(job_id)
        if job_id in self._active_jobs:
            self._active_jobs[job_id].cancel()
            logger.info(f"Cancelled job task {job_id}")

    async def _execute_job(self, job_id: uuid.UUID):
        """Execute a single job."""
        logger.info(f"Executing job {job_id}")

        job: Job | None = None
        try:
            # Fix 3 — Ownership-gated SCHEDULED → RUNNING.
            now = datetime.now(timezone.utc)
            updated = await Job.objects.filter(
                id=job_id,
                state=JobState.SCHEDULED,
                assigned_worker_id=self.worker_id,
            ).aupdate(
                state=JobState.RUNNING,
                started_at=now,
                attempt=F("attempt") + 1,
                updated_at=now,
            )
            if updated == 0:
                logger.warning(
                    f"Job {job_id}: ownership lost before RUNNING transition; aborting"
                )
                return

            # Refresh so event logging has the correct (incremented) attempt count.
            job = await Job.objects.filter(id=job_id).afirst()
            if job is None:
                return

            await record_event(
                job.id,
                "RUNNING",
                detail=f"Execution started (attempt {job.attempt}/{job.max_retries})",
                attempt=job.attempt,
                worker_id=str(self.worker_id),
            )

            result = await self.executor.execute(
                job.job_type, job.payload, job_id=job_id, timeout_seconds=job.timeout_seconds
            )

            # Cancelled during execution — do not write any terminal state.
            if job_id in self._cancelled_jobs:
                self._cancelled_jobs.discard(job_id)
                logger.info(f"Job {job_id} was cancelled during execution.")
                return

            # Fix 1 — All terminal-state writes are ownership-gated CAS.
            now = datetime.now(timezone.utc)
            if result["success"]:
                updated = await Job.objects.filter(
                    id=job_id,
                    state=JobState.RUNNING,
                    assigned_worker_id=self.worker_id,
                ).aupdate(
                    state=JobState.COMPLETED,
                    completed_at=now,
                    result=result.get("result"),
                    updated_at=now,
                )
                if updated == 0:
                    logger.warning(
                        f"Job {job_id}: ownership lost before COMPLETED write; discarding result"
                    )
                    return
                await record_event(
                    job.id,
                    "COMPLETED",
                    detail="Job completed successfully",
                    attempt=job.attempt,
                    worker_id=str(self.worker_id),
                )
                logger.info(f"Job {job_id} completed successfully.")
            else:
                error_msg = result.get("error", "Unknown error")
                logger.warning(
                    f"Job {job_id} failed (attempt {job.attempt}/{job.max_retries}): {error_msg}"
                )
                await record_event(
                    job.id,
                    "FAILED",
                    detail=error_msg,
                    attempt=job.attempt,
                    worker_id=str(self.worker_id),
                )

                if job.attempt < job.max_retries:
                    delay = compute_retry_delay(job.attempt)
                    updated = await Job.objects.filter(
                        id=job_id,
                        state=JobState.RUNNING,
                        assigned_worker_id=self.worker_id,
                    ).aupdate(
                        state=JobState.QUEUED,
                        assigned_worker_id=None,
                        scheduled_at=None,
                        started_at=None,
                        error_message=error_msg,
                        run_after=now + timedelta(seconds=delay),
                        updated_at=now,
                    )
                    if updated == 0:
                        logger.warning(
                            f"Job {job_id}: ownership lost before QUEUED (retry) write"
                        )
                        return
                    await record_event(
                        job.id,
                        "RETRIED",
                        detail=f"Auto-retry: re-queued (attempt {job.attempt}/{job.max_retries})",
                        attempt=job.attempt,
                        worker_id=str(self.worker_id),
                    )
                    logger.info(
                        f"Job {job_id} re-queued for retry "
                        f"(attempt {job.attempt}/{job.max_retries}, delay={delay:.1f}s)"
                    )
                else:
                    updated = await Job.objects.filter(
                        id=job_id,
                        state=JobState.RUNNING,
                        assigned_worker_id=self.worker_id,
                    ).aupdate(
                        state=JobState.DEAD_LETTER,
                        assigned_worker_id=None,
                        error_message=error_msg,
                        updated_at=now,
                    )
                    if updated == 0:
                        logger.warning(
                            f"Job {job_id}: ownership lost before DEAD_LETTER write"
                        )
                        return
                    await DeadLetterJob.objects.acreate(
                        job_id=job.id,
                        final_error=error_msg,
                        total_attempts=job.attempt,
                    )
                    await record_event(
                        job.id,
                        "DEAD_LETTER",
                        detail=f"Moved to dead letter after {job.attempt} attempts: {error_msg}",
                        attempt=job.attempt,
                        worker_id=str(self.worker_id),
                    )
                    logger.error(
                        f"Job {job_id} moved to dead letter after {job.attempt} attempts: {error_msg}"
                    )

        except asyncio.CancelledError:
            logger.info(f"Job {job_id} execution cancelled.")
            t = asyncio.current_task()
            if t is not None:
                while t.cancelling() > 0:
                    t.uncancel()
        except Exception:
            logger.exception(f"Unexpected error executing job {job_id}")
            try:
                updated = await Job.objects.filter(
                    id=job_id,
                    state=JobState.RUNNING,
                    assigned_worker_id=self.worker_id,
                ).aupdate(
                    state=JobState.FAILED,
                    error_message="Worker execution error",
                    updated_at=datetime.now(timezone.utc),
                )
                if updated and job is not None:
                    await record_event(
                        job.id,
                        "FAILED",
                        detail="Worker execution error (unhandled exception)",
                        attempt=job.attempt,
                        worker_id=str(self.worker_id),
                    )
            except Exception:
                logger.exception(f"Failed to mark job {job_id} as failed")
        finally:
            try:
                worker = await Worker.objects.filter(id=self.worker_id).afirst()
                if worker and worker.inflight_job_count > 0:
                    worker.inflight_job_count -= 1
                    if job is not None:
                        tenant_slots = dict(worker.tenant_inflight_job_count_map)
                        tid = str(job.tenant_id)  # type: ignore[attr-defined]
                        if tid in tenant_slots:
                            tenant_slots[tid] = max(0, tenant_slots[tid] - 1)
                            if tenant_slots[tid] == 0:
                                del tenant_slots[tid]
                            worker.tenant_inflight_job_count_map = tenant_slots
                    await worker.asave(
                        update_fields=["inflight_job_count", "tenant_inflight_job_count_map"]
                    )
            except Exception:
                logger.exception(f"Failed to decrement inflight count for job {job_id}")
            self._active_jobs.pop(job_id, None)
            self._cancelled_jobs.discard(job_id)


async def run_worker(max_inflight_jobs: int | None = None):
    """Entry point for running a worker."""
    worker = WorkerNode(max_inflight_jobs=max_inflight_jobs)
    try:
        await worker.start()
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await worker.stop()
