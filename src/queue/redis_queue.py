import json
import time
import uuid

import redis.asyncio as redis

from django.conf import settings
from src.constants import REDIS_JOB_QUEUE, REDIS_WORKER_CHANNEL


class RedisQueue:
    def __init__(self):
        self._redis: redis.Redis | None = None

    async def connect(self):
        self._redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        await self._redis.ping()

    async def _ensure_connected(self) -> redis.Redis:
        """Lazily connect for read-only callers (e.g. API metrics). Workers call connect() explicitly."""
        if self._redis is None:
            await self.connect()
        return self._redis  # type: ignore[return-value]

    async def close(self):
        if self._redis:
            await self._redis.aclose()

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._redis

    # --- Job Queue (Redis Sorted Set) ---
    #
    # Score = enqueue_timestamp (pure FIFO). Lower score = dequeued first (ZPOPMIN).
    # Retried jobs use time.time() and join the back of the queue.
    # Deferred jobs (tenant quota exceeded or no worker available) use created_at
    # to restore their original position when re-inserted by the scheduler.

    async def enqueue_job(
        self, job_id: uuid.UUID, score: float | None = None
    ) -> None:
        """Add a job to the queue. Score defaults to now() if not provided."""
        if score is None:
            score = time.time()
        r = await self._ensure_connected()
        await r.zadd(REDIS_JOB_QUEUE, {str(job_id): score})

    async def dequeue_job(self) -> str | None:
        """Pop the oldest job (lowest score) from the sorted set."""
        # ZPOPMIN returns [(member, score)] or empty list
        r = await self._ensure_connected()
        result = await r.zpopmin(REDIS_JOB_QUEUE, count=1)
        if result:
            return result[0][0]  # member string (job_id)
        return None

    async def queue_length(self) -> int:
        r = await self._ensure_connected()
        return await r.zcard(REDIS_JOB_QUEUE)

    async def peek_queue(self, count: int = 10) -> list[str]:
        """Peek at the next N jobs (oldest first) without removing them."""
        r = await self._ensure_connected()
        result = await r.zrange(REDIS_JOB_QUEUE, 0, count - 1)
        return result

    async def remove_job(self, job_id: uuid.UUID) -> None:
        """Remove a specific job from the queue (e.g., on cancel)"""
        r = await self._ensure_connected()
        await r.zrem(REDIS_JOB_QUEUE, str(job_id))

    # --- Worker Notifications (Pub/Sub) ---

    async def notify_workers(self, message: dict) -> None:
        """Publish a message to the worker notification channel."""
        r = await self._ensure_connected()
        await r.publish(REDIS_WORKER_CHANNEL, json.dumps(message))

    async def subscribe_worker_channel(self):
        "Subscribe to worker notifications. Returns a pubsub object."
        r = await self._ensure_connected()
        pubsub = r.pubsub()
        await pubsub.subscribe(REDIS_WORKER_CHANNEL)
        return pubsub



# Singleton instance
redis_queue = RedisQueue()
