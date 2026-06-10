"""
PostgreSQL-backed job queue.

Replaces the Redis sorted-set queue with a simple DB query.
Enqueue is a no-op: creating a Job row with state=QUEUED is the enqueue.
Dequeue returns the oldest ready job (run_after elapsed or null), FIFO.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from django.db.models import Q

from src.constants import JobState
from src.models import Job


class PgQueue:
    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def enqueue_job(self, job_id: uuid.UUID, **kwargs) -> None:
        """No-op: the Job row with state=QUEUED is the queue entry."""
        pass

    async def dequeue_job(self) -> str | None:
        """Return the ID of the next QUEUED job whose run_after has elapsed."""
        now = datetime.now(timezone.utc)
        job = await (
            Job.objects.filter(state=JobState.QUEUED)
            .filter(Q(run_after__isnull=True) | Q(run_after__lte=now))
            .order_by("created_at")
            .afirst()
        )
        return str(job.id) if job else None

    async def remove_job(self, job_id: uuid.UUID) -> None:
        """No-op: cancellation is handled by state change in the API layer."""
        pass

    async def notify_workers(self, message: dict) -> None:
        """No-op: workers poll the DB directly."""
        pass

    async def queue_length(self) -> int:
        now = datetime.now(timezone.utc)
        return await (
            Job.objects.filter(state=JobState.QUEUED)
            .filter(Q(run_after__isnull=True) | Q(run_after__lte=now))
            .acount()
        )

    async def peek_queue(self, count: int = 10) -> list[str]:
        now = datetime.now(timezone.utc)
        return [
            str(job.id)
            async for job in Job.objects.filter(state=JobState.QUEUED)
            .filter(Q(run_after__isnull=True) | Q(run_after__lte=now))
            .order_by("created_at")[:count]
        ]


pg_queue = PgQueue()
