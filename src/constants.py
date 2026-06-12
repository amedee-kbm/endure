from django.db import models


class JobState(models.TextChoices):
    QUEUED = "QUEUED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


CANCELLABLE_STATES = {
    JobState.QUEUED,
    JobState.SCHEDULED,
    JobState.RUNNING,
}


class WorkerState(models.TextChoices):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"

