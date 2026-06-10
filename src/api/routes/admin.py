import logging
import uuid
from datetime import datetime, timezone

import croniter as croniter_lib
from django.db import IntegrityError
from django.db.models import Count
from ninja import Router, Schema
from ninja.errors import HttpError
from pydantic import Field

from src.constants import JobState
from src.models import DeadLetterJob, Job, PeriodicTask, Tenant
from src.queue.pg_queue import pg_queue
from src.scheduler.leader import LeaderElection

router = Router()
logger = logging.getLogger("endure.api")


@router.get("/health")
def health(request):
    return {"status": "ok"}


@router.get("/queue/stats")
async def queue_stats(request):
    length = await pg_queue.queue_length()
    upcoming = await pg_queue.peek_queue(5)

    state_counts: dict[str, int] = {}
    async for row in Job.objects.values("state").annotate(count=Count("id")):
        state_counts[row["state"]] = row["count"]

    tenant_counts: dict[str, int] = {}
    async for row in Job.objects.filter(
        state__in=[JobState.SCHEDULED, JobState.RUNNING]
    ).values("tenant_id").annotate(count=Count("id")):
        tenant_counts[str(row["tenant_id"])] = row["count"]

    return {
        "queue_length": length,
        "upcoming_jobs": upcoming,
        "job_state_counts": state_counts,
        "tenant_running_counts": tenant_counts,
    }


@router.get("/dead-letter")
async def list_dead_letter_jobs(request, limit: int = 50, offset: int = 0):
    count = await DeadLetterJob.objects.acount()
    items = [
        dl async for dl in DeadLetterJob.objects.all().order_by("-moved_at")[offset : offset + limit]
    ]

    return {
        "total": count,
        "items": [
            {
                "id": str(dl.id),
                "job_id": str(dl.job_id),
                "final_error": dl.final_error,
                "total_attempts": dl.total_attempts,
                "moved_at": dl.moved_at.isoformat(),
            }
            for dl in items
        ],
    }


@router.get("/leader")
async def get_leader(request):
    leader = LeaderElection()
    info = await leader.get_current_leader()
    return {"leader": info}


class TenantRequest(Schema):
    name: str = Field(..., max_length=256)
    max_concurrent_jobs: int = Field(default=32, ge=1, le=1000)
    max_workers: int = Field(default=5, ge=1, le=100)


class TenantResponse(Schema):
    id: uuid.UUID
    name: str
    max_concurrent_jobs: int
    max_workers: int


@router.post("/tenants", response={201: TenantResponse})
async def create_tenant(request, data: TenantRequest):
    existing = await Tenant.objects.filter(name=data.name).afirst()
    if existing:
        raise HttpError(409, "Tenant already exists")

    try:
        tenant = await Tenant.objects.acreate(
            name=data.name,
            max_concurrent_jobs=data.max_concurrent_jobs,
            max_workers=data.max_workers,
        )
    except IntegrityError:
        raise HttpError(409, "Tenant already exists")

    return 201, tenant


@router.put("/tenants/{tenant_id}", response=TenantResponse)
async def update_tenant(request, tenant_id: uuid.UUID, data: TenantRequest):
    tenant = await Tenant.objects.filter(id=tenant_id).afirst()
    if not tenant:
        raise HttpError(404, "Tenant not found")

    tenant.max_concurrent_jobs = data.max_concurrent_jobs
    tenant.max_workers = data.max_workers
    await tenant.asave(update_fields=["max_concurrent_jobs", "max_workers"])
    return tenant


@router.get("/tenants", response=list[TenantResponse])
async def list_tenants(request):
    return [t async for t in Tenant.objects.all()]


@router.get("/tenants/{tenant_id}", response=TenantResponse)
async def get_tenant(request, tenant_id: uuid.UUID):
    tenant = await Tenant.objects.filter(id=tenant_id).afirst()
    if not tenant:
        raise HttpError(404, "Tenant not found")
    return tenant


class PeriodicTaskRequest(Schema):
    name: str = Field(..., max_length=256)
    tenant_id: uuid.UUID
    job_type: str = Field(..., max_length=256)
    cron_expression: str = Field(..., max_length=100)
    payload: dict = Field(default_factory=dict)
    is_active: bool = True


class PeriodicTaskResponse(Schema):
    id: uuid.UUID
    name: str
    tenant_id: uuid.UUID
    job_type: str
    cron_expression: str
    is_active: bool
    next_run_at: datetime


@router.post("/periodic-tasks", response={201: PeriodicTaskResponse})
async def create_periodic_task(request, data: PeriodicTaskRequest):
    tenant = await Tenant.objects.filter(id=data.tenant_id).afirst()
    if not tenant:
        raise HttpError(404, "Tenant not found")

    try:
        now = datetime.now(timezone.utc)
        cron = croniter_lib.croniter(data.cron_expression, now)
        next_run_at = cron.get_next(datetime).replace(tzinfo=timezone.utc)
    except Exception:
        raise HttpError(400, f"Invalid cron expression: {data.cron_expression!r}")

    task = await PeriodicTask.objects.acreate(
        tenant_id=data.tenant_id,
        name=data.name,
        job_type=data.job_type,
        cron_expression=data.cron_expression,
        payload=data.payload,
        is_active=data.is_active,
        next_run_at=next_run_at,
    )
    return 201, task
