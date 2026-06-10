import uuid
from datetime import datetime

from ninja import Router

from src.models import JobEvent

router = Router()


@router.get("")
async def list_events(
    request,
    event: str | None = None,
    job_id: uuid.UUID | None = None,
    job_name: str | None = None,
    worker_id: str | None = None,
    tenant_id: uuid.UUID | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
):
    qs = JobEvent.objects.all().select_related("job").order_by("-timestamp")

    if event is not None:
        qs = qs.filter(event=event)
    if job_id is not None:
        qs = qs.filter(job_id=job_id)
    if job_name is not None:
        qs = qs.filter(job__name__icontains=job_name)
    if worker_id is not None:
        qs = qs.filter(worker_id=worker_id)
    if tenant_id is not None:
        qs = qs.filter(job__tenant_id=tenant_id)
    if since is not None:
        qs = qs.filter(timestamp__gte=since)
    if until is not None:
        qs = qs.filter(timestamp__lte=until)

    total = await qs.acount()
    events = [e async for e in qs[offset : offset + limit]]

    return {
        "total": total,
        "events": [
            {
                "id": str(e.id),
                "job_id": str(e.job_id),
                "job_name": e.job.name if e.job else None,
                "tenant_id": str(e.job.tenant_id) if e.job else None,
                "event": e.event,
                "detail": e.detail,
                "attempt": e.attempt,
                "worker_id": e.worker_id,
                "timestamp": e.timestamp.isoformat(),
                "metadata": e.metadata,
            }
            for e in events
        ],
    }
