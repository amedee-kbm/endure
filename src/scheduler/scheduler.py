"""
Core scheduler loop.
Dequeues jobs from PostgreSQL and assigns them to available workers.
Supports leader election, heartbeat-based failure detection, retry with
exponential backoff, dead-letter queue, and periodic task scheduling.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import croniter

from django.conf import settings
from django.db.models import F

from src.constants import JobState, WorkerState
from src.models import Job, DeadLetterJob, PeriodicTask, Tenant, Worker
from src.queue.pg_queue import pg_queue
from src.scheduler.leader import LeaderElection
from src.scheduler.priority_queue import compute_retry_delay
from src.services.event_logger import record_event

logger = logging.getLogger("src.scheduler.scheduler")

DEFAULT_MAX_CONCURRENT_JOBS = 10


class Scheduler:
    def __init__(self):
        self.leader = LeaderElection()
        self._running = False
        self._heartbeat_counter = 0
        self._tenant_configs: dict = {}

    async def start(self):
        """Main scheduler loop."""
        logger.info(f"Scheduler starting (instance={self.leader.instance_id})...")
        self._running = True

        await self._load_tenant_configs()

        while self._running:
            try:
                if not self.leader.is_leader:
                    acquired = await self.leader.try_acquire()
                    if not acquired:
                        await asyncio.sleep(settings.SCHEDULER_LOOP_INTERVAL)
                        continue
                else:
                    self._heartbeat_counter += 1
                    if self._heartbeat_counter >= int(
                        settings.LEADER_HEARTBEAT_INTERVAL
                        / settings.SCHEDULER_LOOP_INTERVAL
                    ):
                        self._heartbeat_counter = 0
                        still_leader = await self.leader.renew_heartbeat()
                        if not still_leader:
                            continue

                await self._enqueue_periodic_tasks()
                await self._schedule_cycle()

            except Exception:
                logger.exception("Error in scheduler loop")

            await asyncio.sleep(settings.SCHEDULER_LOOP_INTERVAL)

    async def stop(self):
        self._running = False
        logger.info("Scheduler stopping...")

    async def _load_tenant_configs(self):
        configs = [t async for t in Tenant.objects.all()]
        self._tenant_configs = {c.id: c for c in configs}
        logger.info(f"Loaded {len(self._tenant_configs)} tenant configs.")

    def _get_tenant_max_concurrent(self, tenant_id) -> int:
        config = self._tenant_configs.get(tenant_id)
        return config.max_concurrent_jobs if config else DEFAULT_MAX_CONCURRENT_JOBS

    async def _get_tenant_running_count(self, tenant_id) -> int:
        return await Job.objects.filter(
            tenant_id=tenant_id, state__in=[JobState.SCHEDULED, JobState.RUNNING]
        ).acount()

    async def _enqueue_periodic_tasks(self):
        """Find active periodic tasks due to run and create jobs for them."""
        now_dt = datetime.now(timezone.utc)
        tasks = [
            t async for t in PeriodicTask.objects.filter(
                is_active=True, next_run_at__lte=now_dt
            )
        ]

        enqueued_count = 0
        for task in tasks:
            new_job = await Job.objects.acreate(
                tenant_id=task.tenant_id,  # type: ignore[attr-defined]
                name=task.name,
                job_type=task.job_type,
                payload=task.payload,
                state=JobState.QUEUED,
                periodic_task_id=task.id,
            )

            await record_event(
                new_job.id, "QUEUED", detail=f"Created from periodic task '{task.name}'"
            )

            cron = croniter.croniter(task.cron_expression, now_dt)
            task.last_run_at = now_dt
            task.next_run_at = cron.get_next(datetime).replace(tzinfo=timezone.utc)
            enqueued_count += 1
            await task.asave(update_fields=["last_run_at", "next_run_at"])

        if enqueued_count > 0:
            logger.info(f"Enqueued {enqueued_count} periodic tasks.")

    async def _schedule_cycle(self):
        """One scheduling cycle: detect failures, dequeue, assign."""
        await self._detect_dead_workers()
        await self._detect_timed_out_jobs()

        await self._load_tenant_configs()

        assigned = 0

        while True:
            job_id_str = await pg_queue.dequeue_job()
            if not job_id_str:
                break

            import uuid
            job_id = uuid.UUID(job_id_str)
            job = await Job.objects.filter(id=job_id).afirst()
            if not job or job.state != JobState.QUEUED:
                continue

            tenant_id = job.tenant_id  # type: ignore[attr-defined]
            max_concurrent = self._get_tenant_max_concurrent(tenant_id)
            current_count = await self._get_tenant_running_count(tenant_id)
            if current_count >= max_concurrent:
                # Job stays QUEUED in DB; next cycle will reconsider it.
                logger.debug(
                    f"Job {job.id} deferred: tenant {tenant_id} at quota "
                    f"({current_count}/{max_concurrent})"
                )
                break

            worker = await self._find_available_worker()

            if not worker:
                break

            job.state = JobState.SCHEDULED
            job.assigned_worker = worker
            job.scheduled_at = datetime.now(timezone.utc)
            worker.inflight_job_count += 1

            tenant_slots = dict(worker.tenant_inflight_job_count_map)
            tenant_key = str(job.tenant_id)  # type: ignore[attr-defined]
            tenant_slots[tenant_key] = tenant_slots.get(tenant_key, 0) + 1
            worker.tenant_inflight_job_count_map = tenant_slots

            await worker.asave(update_fields=["inflight_job_count", "tenant_inflight_job_count_map"])
            await job.asave(update_fields=["state", "assigned_worker_id", "scheduled_at"])
            await record_event(
                job.id,
                "SCHEDULED",
                detail=f"Assigned to worker {worker.id} ({worker.hostname})",
                attempt=job.attempt,
                worker_id=str(worker.id),
            )

            assigned += 1
            logger.info(
                f"Assigned job {job.id} ({job.name}, tenant={tenant_id}) to worker {worker.id}"
            )

        if assigned:
            logger.info(f"Scheduling cycle: {assigned} jobs assigned.")

    async def _find_available_worker(self) -> Worker | None:
        return await (
            Worker.objects.filter(
                state=WorkerState.ONLINE, inflight_job_count__lt=F("max_inflight_jobs")
            )
            .order_by("inflight_job_count")
            .afirst()
        )

    async def _detect_dead_workers(self):
        cutoff = datetime.now(timezone.utc).timestamp() - settings.WORKER_HEARTBEAT_TIMEOUT
        cutoff_dt = datetime.fromtimestamp(cutoff, tz=timezone.utc)

        dead_workers = [
            w async for w in Worker.objects.filter(
                state=WorkerState.ONLINE, last_heartbeat__lt=cutoff_dt
            )
        ]

        for worker in dead_workers:
            logger.warning(
                f"Worker {worker.id} ({worker.hostname}) missed heartbeat, marking OFFLINE."
            )
            worker.state = WorkerState.OFFLINE
            await worker.asave(update_fields=["state"])

            orphaned_jobs = [
                j async for j in Job.objects.filter(
                    assigned_worker=worker,
                    state__in=[JobState.SCHEDULED, JobState.RUNNING],
                )
            ]
            for job in orphaned_jobs:
                await self._handle_job_failure(job, error="Worker died (missed heartbeat)")

    async def _detect_timed_out_jobs(self):
        now = datetime.now(timezone.utc)
        running_jobs = [
            j async for j in Job.objects.filter(
                state=JobState.RUNNING, started_at__isnull=False
            )
        ]

        for job in running_jobs:
            elapsed = (now - job.started_at).total_seconds()
            if elapsed > job.timeout_seconds:
                logger.warning(
                    f"Job {job.id} timed out after {elapsed:.0f}s (limit={job.timeout_seconds}s)"
                )
                job.state = JobState.TIMED_OUT
                await job.asave(update_fields=["state"])
                await record_event(
                    job.id,
                    "TIMED_OUT",
                    detail=f"Timed out after {elapsed:.0f}s (limit={job.timeout_seconds}s)",
                    attempt=job.attempt,
                    worker_id=str(job.assigned_worker_id) if job.assigned_worker_id else None,  # type: ignore[attr-defined]
                )
                await self._handle_job_failure(job, error=f"Timed out after {elapsed:.0f}s")

    async def _handle_job_failure(self, job: Job, error: str):
        job.error_message = error
        old_worker_id = job.assigned_worker_id  # type: ignore[attr-defined]
        job.assigned_worker = None

        if job.attempt < job.max_retries:
            delay = compute_retry_delay(job.attempt)
            job.state = JobState.QUEUED
            job.scheduled_at = None
            job.run_after = datetime.now(timezone.utc) + timedelta(seconds=delay)
            await job.asave(
                update_fields=["error_message", "assigned_worker_id", "state", "scheduled_at", "run_after"]
            )
            await record_event(
                job.id,
                "RETRIED",
                detail=f"Auto-retry by scheduler (attempt {job.attempt}/{job.max_retries}): {error}",
                attempt=job.attempt,
                worker_id=str(old_worker_id) if old_worker_id else None,
            )
            logger.info(
                f"Re-queuing job {job.id} for retry (attempt {job.attempt}/{job.max_retries}, "
                f"delay={delay:.1f}s)"
            )
        else:
            job.state = JobState.DEAD_LETTER
            await job.asave(update_fields=["error_message", "assigned_worker_id", "state"])
            await DeadLetterJob.objects.acreate(
                job=job,
                final_error=error,
                total_attempts=job.attempt,
            )
            await record_event(
                job.id,
                "DEAD_LETTER",
                detail=f"Moved to dead letter after {job.attempt} attempts: {error}",
                attempt=job.attempt,
                worker_id=str(old_worker_id) if old_worker_id else None,
            )
            logger.error(
                f"Job {job.id} moved to dead letter after {job.attempt} attempts: {error}"
            )

        if old_worker_id:
            worker = await Worker.objects.filter(id=old_worker_id).afirst()
            if worker:
                if worker.inflight_job_count > 0:
                    worker.inflight_job_count -= 1
                tenant_slots = dict(worker.tenant_inflight_job_count_map)
                tenant_key = str(job.tenant_id)  # type: ignore[attr-defined]
                if tenant_key in tenant_slots:
                    tenant_slots[tenant_key] = max(0, tenant_slots[tenant_key] - 1)
                    if tenant_slots[tenant_key] == 0:
                        del tenant_slots[tenant_key]
                    worker.tenant_inflight_job_count_map = tenant_slots
                await worker.asave(
                    update_fields=["inflight_job_count", "tenant_inflight_job_count_map"]
                )


async def run_scheduler():
    """Entry point for running the scheduler."""
    scheduler = Scheduler()
    try:
        await scheduler.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        await scheduler.stop()
