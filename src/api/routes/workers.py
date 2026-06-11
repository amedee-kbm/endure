from django.db.models import Count, Q
from ninja import Router

from src.api.schemas import WorkerListResponse
from src.constants import JobState
from src.models import Worker

router = Router()


@router.get("", response=WorkerListResponse)
async def list_workers(request, state: str | None = None):
    # inflight_job_count is derived, not stored; the schema field is fed by
    # this annotation
    qs = Worker.objects.annotate(
        inflight_job_count=Count(
            "assigned_jobs",
            filter=Q(assigned_jobs__state__in=[JobState.SCHEDULED, JobState.RUNNING]),
        )
    ).order_by("-registered_at")
    if state is not None:
        qs = qs.filter(state=state)
    workers = [w async for w in qs]
    return WorkerListResponse(workers=workers, total=len(workers))
