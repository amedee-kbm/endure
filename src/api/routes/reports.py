"""
Reports API — thin domain wrapper over the core jobs API.

Hides job_type strings from callers; exposes
a report-centric interface (report_type, tenant_id, payload).

Endpoints:
  POST /v1/reports              — submit a report job
  GET  /v1/reports              — list report jobs (with optional tenant_id filter)
  GET  /v1/reports/{job_id}     — single report job status + artifact path
"""

import uuid
from datetime import datetime
from typing import Literal

from ninja import Router, Schema
from ninja.errors import HttpError

from src.constants import JobState
from src.models import Job, Tenant
from src.queue.redis_queue import redis_queue
from src.scheduler.priority_queue import compute_queue_score
from src.services.event_logger import record_event

router = Router()

REPORT_REGISTRY: dict[str, str] = {
    "daily_import": "src.reporting.jobs.daily_import:DailyImportJob",
}

REPORT_JOB_TYPE_PREFIX = "src.reporting.jobs."


class ReportSubmitRequest(Schema):
    tenant_id: uuid.UUID
    report_type: Literal["daily_import"]
    payload: dict = {}
    max_retries: int = 3
    timeout_seconds: int = 1800


class ReportResponse(Schema):
    job_id: uuid.UUID
    report_type: str
    tenant_id: uuid.UUID
    state: str
    artifact_path: str | None
    created_at: datetime
    updated_at: datetime


def _artifact_from_job(job: Job) -> str | None:
    result = getattr(job, "result", None) or {}
    if isinstance(result, dict):
        return result.get("artifact_path")
    return None


def _report_type_from_job(job: Job) -> str:
    for rt, jt in REPORT_REGISTRY.items():
        if job.job_type == jt:
            return rt
    return job.job_type


def _job_to_report_response(job: Job) -> ReportResponse:
    return ReportResponse(
        job_id=job.id,
        report_type=_report_type_from_job(job),
        tenant_id=job.tenant_id,
        state=job.state,
        artifact_path=_artifact_from_job(job),
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


@router.post("", response={201: ReportResponse})
async def submit_report(request, data: ReportSubmitRequest):
    tenant = await Tenant.objects.filter(id=data.tenant_id).afirst()
    if not tenant:
        raise HttpError(404, "Tenant not found")

    if data.report_type not in REPORT_REGISTRY:
        raise HttpError(400, f"Unknown report_type: {data.report_type!r}")

    job_type = REPORT_REGISTRY[data.report_type]
    payload = {**data.payload, "tenant_id": str(data.tenant_id)}

    job = await Job.objects.acreate(
        tenant_id=data.tenant_id,
        name=f"{data.report_type}-{data.tenant_id}",
        job_type=job_type,
        payload=payload,
        state=JobState.SUBMITTED,
        max_retries=data.max_retries,
        timeout_seconds=data.timeout_seconds,
    )

    await record_event(job.id, "CREATED", detail=f"Report submitted: {data.report_type}")
    job.state = JobState.QUEUED
    await job.asave(update_fields=["state", "updated_at"])
    await record_event(job.id, "QUEUED", detail="Enqueued for scheduling")

    score = compute_queue_score()
    await redis_queue.enqueue_job(job.id, score=score)

    return 201, _job_to_report_response(job)


@router.get("", response=list[ReportResponse])
async def list_reports(
    request,
    tenant_id: uuid.UUID | None = None,
    state: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    qs = Job.objects.filter(
        job_type__startswith=REPORT_JOB_TYPE_PREFIX
    ).order_by("-created_at")

    if tenant_id is not None:
        qs = qs.filter(tenant_id=tenant_id)
    if state is not None:
        qs = qs.filter(state=state)

    jobs = [j async for j in qs[offset: offset + limit]]
    return [_job_to_report_response(j) for j in jobs]


@router.get("/{job_id}", response=ReportResponse)
async def get_report(request, job_id: uuid.UUID):
    job = await Job.objects.filter(
        id=job_id, job_type__startswith=REPORT_JOB_TYPE_PREFIX
    ).afirst()
    if not job:
        raise HttpError(404, "Report not found")
    return _job_to_report_response(job)
