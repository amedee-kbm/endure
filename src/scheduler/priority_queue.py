"""
FIFO job queue scoring and retry delay calculation.

Score formula for Redis sorted set (ZPOPMIN takes lowest score first):
  score = enqueue_timestamp (or enqueue_timestamp + backoff_delay for retries)

Earlier jobs (lower timestamp) have lower scores and are dequeued first.
Retried jobs use time.time() + backoff_delay so they are not dispatched
before the delay elapses and do not jump ahead of jobs submitted while
they were failing.
"""

import random
import time

from django.conf import settings


def compute_queue_score(enqueue_time: float | None = None) -> float:
    """Lower score = dequeued first (ZPOPMIN). Pure FIFO by submission time."""
    if enqueue_time is None:
        enqueue_time = time.time()
    return enqueue_time


def compute_retry_delay(attempt: int) -> float:
    """Exponential backoff with jitter: min(base * 2^attempt + jitter, max_delay)."""
    delay = settings.RETRY_BASE_DELAY * (2**attempt)
    jitter = random.uniform(0, settings.RETRY_BASE_DELAY)
    return min(delay + jitter, settings.RETRY_MAX_DELAY)
