"""
Helper to record job events from anywhere in the codebase.
"""

import uuid
import logging

from src.models import JobEvent

logger = logging.getLogger("src.events")


async def record_event(
    job_id: uuid.UUID,
    event: str,
    detail: str | None = None,
    attempt: int | None = None,
    worker_id: str | None = None,
    metadata: dict | None = None,
) -> JobEvent:
    """
    Append an event to the job's audit log.

    Common event names:
      CREATED, QUEUED, SCHEDULED, RUNNING, COMPLETED,
      FAILED, DEAD_LETTER, CANCELLED, TIMED_OUT, MANUAL_RETRY
    """
    return await JobEvent.objects.acreate(
        job_id=job_id,
        event=event,
        detail=detail,
        attempt=attempt,
        worker_id=worker_id,
        metadata=metadata,
    )
