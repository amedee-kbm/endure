from django.db import models


class JobState(models.TextChoices):
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


CANCELLABLE_STATES = {
    JobState.SUBMITTED,
    JobState.QUEUED,
    JobState.SCHEDULED,
    JobState.RUNNING,
}


class WorkerState(models.TextChoices):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"

