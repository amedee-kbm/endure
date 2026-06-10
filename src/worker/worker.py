"""
Worker main loop.
Listens for job assignments via Redis pub/sub, executes them, reports results.
Handles cancellation notifications.
"""

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timezone

from django.conf import settings
from django.db.models import F

from src.constants import JobState, WorkerState
from src.models import DeadLetterJob, Job, Worker
from src.queue.redis_queue import redis_queue
from src.scheduler.priority_queue import compute_queue_score, compute_retry_delay
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
        await redis_queue.connect()

        # Register in database
        await Worker.objects.acreate(
            id=self.worker_id,
            hostname=self.hostname,
            pid=self.pid,
            max_inflight_jobs=self.max_inflight_jobs,
            state=WorkerState.ONLINE,
            last_heartbeat=datetime.now(timezone.utc),
        )

        self._running = True

        # Start heartbeat, pub/sub listener, and polling concurrently
        await asyncio.gather(
            self.heartbeat.start(),
            self._listen_for_jobs(),
            self._poll_for_assigned_jobs(),
        )

    async def stop(self):
        """Gracefully shut down the worker."""
        self._running = False
        self.heartbeat.stop()

        # Cancel all active job tasks
        for job_id, task in self._active_jobs.items():
            task.cancel()
            logger.info(f"Cancelled active job {job_id}")

        worker = await Worker.objects.filter(id=self.worker_id).afirst()
        if worker:
            worker.state = WorkerState.OFFLINE
            await worker.asave(update_fields=["state"])

        await redis_queue.close()
        logger.info(f"Worker {self.worker_id} stopped.")

    async def _listen_for_jobs(self):
        """Listen for job assignment notifications via Redis pub/sub."""
        pubsub = await redis_queue.subscribe_worker_channel()

        while self._running:
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message and message["type"] == "message":
                    data = json.loads(message["data"])
                    await self._handle_notification(data)
            except Exception:
                logger.exception("Error in pub/sub listener")
                await asyncio.sleep(1)

    async def _poll_for_assigned_jobs(self):
        """Fallback polling: check DB for assigned jobs in case pub/sub missed them."""
        while self._running:
            try:
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
            except Exception:
                logger.exception("Error in job polling")

            await asyncio.sleep(settings.SCHEDULER_LOOP_INTERVAL * 2)

    async def _handle_notification(self, data: dict):
        """Handle a notification from the scheduler."""
        msg_type = data.get("type")

        if msg_type == "job_assigned":
            worker_id = data.get("worker_id")
            if worker_id == str(self.worker_id):
                job_id = uuid.UUID(data["job_id"])
                if job_id not in self._active_jobs:
                    task = asyncio.create_task(self._execute_job(job_id))
                    self._active_jobs[job_id] = task

        elif msg_type == "cancel":
            job_id = uuid.UUID(data["job_id"])
            self._cancel_job(job_id)

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
            # Both the state and assigned_worker must match this worker.  If
            # another actor (coordinator sweep, cancel) has already changed
            # either field, aupdate returns 0 and we abort without executing.
            # Using F("attempt")+1 makes the increment atomic in SQL and avoids
            # the double-increment that occurs when two workers race.
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

            # Execute the job (passing job_id enables checkpointing)
            result = await self.executor.execute(
                job.job_type, job.payload, job_id=job_id, timeout_seconds=job.timeout_seconds
            )

            # Cancelled during execution — do not write any terminal state.
            if job_id in self._cancelled_jobs:
                self._cancelled_jobs.discard(job_id)
                logger.info(f"Job {job_id} was cancelled during execution.")
                return

            # Fix 1 — All terminal-state writes are ownership-gated CAS.
            # If updated == 0 the coordinator re-dispatched this job while we
            # were executing (false-suspect scenario): another worker now owns
            # the slot, so we discard our result and return silently.
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
                    delay = compute_retry_delay(job.attempt)
                    score = compute_queue_score(time.time() + delay)
                    await redis_queue.enqueue_job(job.id, score=score)
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
            # Always decrement worker load so slots are freed on completion, failure, or cancellation.
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
        # Keep running until cancelled
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await worker.stop()
