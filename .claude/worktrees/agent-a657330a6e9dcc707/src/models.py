import uuid
from django.db import models

from .constants import JobState, WorkerState

# ---------------------------------------------------------------------------
# Tenant
# ---------------------------------------------------------------------------


class Tenant(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=256, unique=True)
    max_concurrent_jobs = models.IntegerField(default=10)
    max_workers = models.IntegerField(default=5)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "tenants"

    def __repr__(self) -> str:
        return (
            f"<Tenant {self.id} name={self.name} max_jobs={self.max_concurrent_jobs}>"
        )


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class Worker(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    hostname = models.CharField(max_length=256)
    pid = models.IntegerField()
    # Runtime counter: {tenant_id: inflight_job_count} — updated on assign/complete/fail
    tenant_inflight_job_count_map = models.JSONField(default=dict)
    max_inflight_jobs = models.IntegerField(default=4)
    inflight_job_count = models.IntegerField(default=0)
    state = models.CharField(
        max_length=16,
        choices=WorkerState.choices,
        default=WorkerState.ONLINE,
    )
    last_heartbeat = models.DateTimeField()
    registered_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "workers"

    def __repr__(self) -> str:
        return f"<Worker {self.id} host={self.hostname} state={self.state}>"


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


class Job(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        related_name="jobs",
        db_index=True,
    )
    name = models.CharField(max_length=256)
    payload = models.JSONField(default=dict)
    job_type = models.CharField(max_length=256)
    state = models.CharField(
        max_length=16,
        choices=JobState.choices,
        default=JobState.SUBMITTED,
        db_index=True,
    )
    attempt = models.IntegerField(default=0)
    max_retries = models.IntegerField(default=3)
    timeout_seconds = models.IntegerField(default=3600)
    error_message = models.TextField(null=True, blank=True)
    result = models.JSONField(null=True, blank=True, default=None)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    scheduled_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    assigned_worker = models.ForeignKey(
        Worker,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="assigned_jobs",
        db_column="assigned_worker_id",
    )
    periodic_task = models.ForeignKey(
        "PeriodicTask",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="spawned_jobs",
        db_column="periodic_task_id",
    )

    class Meta:
        db_table = "jobs"

    def __repr__(self) -> str:
        return f"<Job {self.id} name={self.name} state={self.state}>"

    async def get_latest_checkpoint(self):
        return await self.checkpoints.order_by("-sequence_number").afirst()


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


class Checkpoint(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(
        Job,
        on_delete=models.CASCADE,
        related_name="checkpoints",
        db_column="job_id",
        db_index=True,
    )
    sequence_number = models.IntegerField()
    storage_path = models.TextField()
    size_bytes = models.BigIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "checkpoints"
        ordering = ["sequence_number"]

    def __repr__(self) -> str:
        return f"<Checkpoint {self.id} job={self.job_id} seq={self.sequence_number}>"


# ---------------------------------------------------------------------------
# DeadLetterJob
# ---------------------------------------------------------------------------


class DeadLetterJob(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.OneToOneField(
        Job,
        on_delete=models.CASCADE,
        related_name="dead_letter",
        db_column="job_id",
    )
    final_error = models.TextField(null=True, blank=True)
    total_attempts = models.IntegerField()
    moved_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "dead_letter_jobs"

    def __repr__(self) -> str:
        return f"<DeadLetterJob job={self.job_id} attempts={self.total_attempts}>"


# ---------------------------------------------------------------------------
# JobEvent
# ---------------------------------------------------------------------------


class JobEvent(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(
        Job,
        on_delete=models.CASCADE,
        related_name="events",
        db_column="job_id",
        db_index=True,
    )
    event = models.CharField(max_length=64)
    detail = models.TextField(null=True, blank=True)
    attempt = models.IntegerField(null=True, blank=True)
    worker_id = models.CharField(max_length=128, null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = "job_events"

    def __repr__(self) -> str:
        return f"<JobEvent {self.event} job={self.job_id} at={self.timestamp}>"


# ---------------------------------------------------------------------------
# SchedulerLeader
# ---------------------------------------------------------------------------


class SchedulerLeader(models.Model):
    # Single-row lock table; always upsert with id=1
    id = models.IntegerField(primary_key=True)
    holder_id = models.CharField(max_length=64)
    acquired_at = models.DateTimeField()
    renewed_at = models.DateTimeField()

    class Meta:
        db_table = "scheduler_leader"

    def __repr__(self) -> str:
        return f"<SchedulerLeader holder={self.holder_id}>"


# ---------------------------------------------------------------------------
# PeriodicTask
# ---------------------------------------------------------------------------


class PeriodicTask(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        related_name="periodic_tasks",
        db_index=True,
    )
    name = models.CharField(max_length=256)
    job_type = models.CharField(max_length=256)
    payload = models.JSONField(default=dict)
    cron_expression = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)

    last_run_at = models.DateTimeField(null=True, blank=True)
    next_run_at = models.DateTimeField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "periodic_tasks"

    def __repr__(self) -> str:
        return f"<PeriodicTask {self.id} name={self.name} cron={self.cron_expression}>"


# ---------------------------------------------------------------------------
# StepOutput
# ---------------------------------------------------------------------------


class StepOutput(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(
        Job,
        on_delete=models.CASCADE,
        related_name="step_outputs",
        db_column="job_id",
        db_index=True,
    )
    step_id = models.IntegerField()
    step_name = models.CharField(max_length=256)
    output = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "step_outputs"
        unique_together = [("job", "step_id")]

    def __repr__(self) -> str:
        return f"<StepOutput job={self.job_id} step={self.step_id} name={self.step_name}>"
