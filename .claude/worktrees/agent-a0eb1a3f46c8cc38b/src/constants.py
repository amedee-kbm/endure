from django.db import models


class JobPriority(models.TextChoices):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"


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
    PREEMPTED = "PREEMPTED"
    DEAD_LETTER = "DEAD_LETTER"


PRIORITY_WEIGHTS = {
    JobPriority.CRITICAL: 3,
    JobPriority.HIGH: 2,
    JobPriority.NORMAL: 1,
}


class WorkerState(models.TextChoices):
    ONLINE = "ONLINE"
    DRAINING = "DRAINING"
    OFFLINE = "OFFLINE"


VALID_TRANSITIONS = {
    JobState.SUBMITTED: {JobState.QUEUED, JobState.CANCELLED},
    JobState.QUEUED: {JobState.SCHEDULED, JobState.CANCELLED},
    JobState.SCHEDULED: {JobState.RUNNING, JobState.PREEMPTED, JobState.CANCELLED},
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
    JobState.PREEMPTED: {JobState.QUEUED},
    JobState.COMPLETED: set(),
    JobState.CANCELLED: set(),
    JobState.DEAD_LETTER: set(),
}

REDIS_JOB_QUEUE = "endure:queue:jobs"
REDIS_WORKER_CHANNEL = "endure:channel:workers"
REDIS_JOB_LOCK_PREFIX = "endure:lock:job:"
