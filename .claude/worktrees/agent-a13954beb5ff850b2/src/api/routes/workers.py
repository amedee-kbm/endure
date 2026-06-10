from ninja import Router

from src.api.schemas import WorkerListResponse
from src.models import Worker

router = Router()


@router.get("", response=WorkerListResponse)
async def list_workers(request, state: str | None = None):
    qs = Worker.objects.all().order_by("-registered_at")
    if state is not None:
        qs = qs.filter(state=state)
    workers = [w async for w in qs]
    return WorkerListResponse(workers=workers, total=len(workers))
