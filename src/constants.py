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
    CHECKPOINTED = "CHECKPOINTED"
    DEAD_LETTER = "DEAD_LETTER"


class WorkerState(models.TextChoices):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


VALID_TRANSITIONS = {
    JobState.SUBMITTED: {JobState.QUEUED, JobState.CANCELLED},
    JobState.QUEUED: {JobState.SCHEDULED, JobState.CANCELLED},
    JobState.SCHEDULED: {JobState.RUNNING, JobState.CANCELLED},
    JobState.RUNNING: {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.TIMED_OUT,
        JobState.CHECKPOINTED,
        JobState.CANCELLED,
    },
    JobState.CHECKPOINTED: {JobState.RUNNING},
    JobState.FAILED: {JobState.QUEUED, JobState.DEAD_LETTER},
    JobState.TIMED_OUT: {JobState.QUEUED, JobState.DEAD_LETTER},
    JobState.COMPLETED: set(),
    JobState.CANCELLED: set(),
    JobState.DEAD_LETTER: set(),
}

