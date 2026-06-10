"""Retry delay calculation for job requeuing."""

import random

from django.conf import settings


def compute_retry_delay(attempt: int) -> float:
    """Exponential backoff with jitter: min(base * 2^attempt + jitter, max_delay)."""
    delay = settings.RETRY_BASE_DELAY * (2**attempt)
    jitter = random.uniform(0, settings.RETRY_BASE_DELAY)
    return min(delay + jitter, settings.RETRY_MAX_DELAY)
