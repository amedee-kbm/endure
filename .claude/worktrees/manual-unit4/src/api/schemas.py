import uuid
from datetime import datetime

from ninja import Schema
from pydantic import Field


class JobSubmitRequest(Schema):
    name: str = Field(..., max_length=256)
    tenant_id: uuid.UUID
    job_type: str = Field(..., max_length=256)
    payload: dict = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0, le=100)
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)


class JobResponse(Schema):
    id: uuid.UUID
    name: str
    tenant_id: uuid.UUID
    job_type: str
    payload: dict
    state: str
    attempt: int
    max_retries: int
    timeout_seconds: int
    result: dict | None = None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    assigned_worker_id: uuid.UUID | None
    periodic_task_id: uuid.UUID | None


class JobListResponse(Schema):
    jobs: list[JobResponse]
    total: int


class WorkerResponse(Schema):
    id: uuid.UUID
    hostname: str
    pid: int
    max_inflight_jobs: int
    inflight_job_count: int
    state: str
    last_heartbeat: datetime
    registered_at: datetime


class WorkerListResponse(Schema):
    workers: list[WorkerResponse]
    total: int
