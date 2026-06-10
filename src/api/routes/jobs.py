import uuid
from datetime import datetime, timezone

from asgiref.sync import sync_to_async
from ninja import Router
from ninja.errors import HttpError

from src.api.schemas import JobListResponse, JobResponse, JobSubmitRequest
from src.constants import CANCELLABLE_STATES, JobState
from src.models import Checkpoint, DeadLetterJob, Job, JobEvent, StepOutput, Tenant
from src.services.event_logger import record_event

router = Router()

RETRYABLE_STATES = {JobState.DEAD_LETTER}


@router.post("", response={201: JobResponse})
async def submit_job(request, data: JobSubmitRequest):
    tenant = await Tenant.objects.filter(id=data.tenant_id).afirst()
    if not tenant:
        raise HttpError(404, "Tenant not found")

    job = await Job.objects.acreate(
        tenant_id=data.tenant_id,
        name=data.name,
        job_type=data.job_type,
        payload=data.payload,
        state=JobState.SUBMITTED,
        max_retries=data.max_retries,
        timeout_seconds=data.timeout_seconds,
    )

    await record_event(job.id, "CREATED", detail=f"Job submitted: {data.name}")

    job.state = JobState.QUEUED
    await job.asave(update_fields=["state", "updated_at"])
    await record_event(job.id, "QUEUED", detail="Enqueued for scheduling")

    return 201, job


@router.get("/{job_id}", response=JobResponse)
async def get_job(request, job_id: uuid.UUID):
    job = await Job.objects.filter(id=job_id).afirst()
    if not job:
        raise HttpError(404, "Job not found")
    return job


@router.get("", response=JobListResponse)
async def list_jobs(
    request,
    tenant_id: uuid.UUID | None = None,
    state: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    qs = Job.objects.all().order_by("-created_at")
    if tenant_id is not None:
        qs = qs.filter(tenant_id=tenant_id)
    if state is not None:
        qs = qs.filter(state=state)

    total = await qs.acount()
    jobs = [j async for j in qs[offset : offset + limit]]

    return JobListResponse(jobs=jobs, total=total)


@router.post("/{job_id}/cancel", response=JobResponse)
async def cancel_job(request, job_id: uuid.UUID):
    job = await Job.objects.filter(id=job_id).afirst()
    if not job:
        raise HttpError(404, "Job not found")

    if job.state not in CANCELLABLE_STATES:
        raise HttpError(409, f"Cannot cancel job in state {job.state}")

    job.state = JobState.CANCELLED
    job.completed_at = datetime.now(timezone.utc)
    await job.asave(update_fields=["state", "completed_at", "updated_at"])
    await record_event(job.id, "CANCELLED", detail="Job cancelled by user")

    return job


@router.post("/{job_id}/retry", response=JobResponse)
async def retry_job(request, job_id: uuid.UUID):
    job = await Job.objects.filter(id=job_id).afirst()
    if not job:
        raise HttpError(404, "Job not found")

    if job.state not in RETRYABLE_STATES:
        raise HttpError(
            409,
            f"Cannot retry job in state {job.state}. Job must be in DEAD_LETTER.",
        )

    await record_event(
        job.id,
        "MANUAL_RETRY",
        detail=f"Manual retry from state {job.state}",
        attempt=job.attempt,
    )

    await DeadLetterJob.objects.filter(job_id=job_id).adelete()

    job.state = JobState.QUEUED
    job.attempt = 0
    job.error_message = None
    job.assigned_worker_id = None
    job.scheduled_at = None
    job.started_at = None
    job.completed_at = None
    await job.asave(
        update_fields=[
            "state",
            "attempt",
            "error_message",
            "assigned_worker",
            "scheduled_at",
            "started_at",
            "completed_at",
            "updated_at",
        ]
    )
    await record_event(job.id, "QUEUED", detail="Re-queued after manual retry")

    return job


@router.get("/{job_id}/events")
async def get_job_events(request, job_id: uuid.UUID):
    job = await Job.objects.filter(id=job_id).afirst()
    if not job:
        raise HttpError(404, "Job not found")

    events = [e async for e in JobEvent.objects.filter(job_id=job_id).order_by("timestamp")]

    return [
        {
            "id": str(e.id),
            "event": e.event,
            "detail": e.detail,
            "attempt": e.attempt,
            "worker_id": e.worker_id,
            "timestamp": e.timestamp.isoformat(),
            "metadata": e.metadata,
        }
        for e in events
    ]


@router.get("/{job_id}/step-outputs")
async def get_step_outputs(request, job_id: uuid.UUID):
    job = await Job.objects.filter(id=job_id).afirst()
    if not job:
        raise HttpError(404, "Job not found")

    outputs = await sync_to_async(list)(
        StepOutput.objects.filter(job=job).order_by("step_id").values(
            "step_id", "step_name", "created_at"
        )
    )
    return {"job_id": str(job_id), "step_outputs": outputs, "count": len(outputs)}


@router.get("/{job_id}/checkpoints")
async def get_job_checkpoints(request, job_id: uuid.UUID):
    job = await Job.objects.filter(id=job_id).afirst()
    if not job:
        raise HttpError(404, "Job not found")

    checkpoints = [
        cp async for cp in Checkpoint.objects.filter(job_id=job_id).order_by("-sequence_number")
    ]
    latest_cp = await job.get_latest_checkpoint()

    return {
        "job_id": str(job_id),
        "latest_checkpoint_id": str(latest_cp.id) if latest_cp else None,
        "total": len(checkpoints),
        "checkpoints": [
            {
                "id": str(cp.id),
                "sequence_number": cp.sequence_number,
                "size_bytes": cp.size_bytes,
                "created_at": cp.created_at.isoformat(),
                "is_latest": latest_cp is not None and cp.id == latest_cp.id,
            }
            for cp in checkpoints
        ],
    }
