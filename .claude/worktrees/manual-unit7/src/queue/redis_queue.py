import json
import time
import uuid

import redis.asyncio as redis

from django.conf import settings
from src.constants import REDIS_JOB_QUEUE, REDIS_WORKER_CHANNEL, REDIS_JOB_LOCK_PREFIX

class RedisQueue:
  def __init__(self):
    self._redis: redis.Redis | None = None

  async def connect(self):
    self._redis = redis.from_url(settings.redis.url, decode_responses=True)
    await self._redis.ping()

  async def close(self):
    if self._redis:
      await self._redis.aclose()

  @property
  def redis(self) -> redis.Redis:
    if self._redis is None:
      raise RuntimeError("Redis not connected. Call connect() first.")
    return self._redis
  
  # --- Priority Job Queue (Redis Sorted Set) ---
  #
  # Score formula: -(priority_weight * 1000) + enqueue_timestamp where timestamp is time.time() epoch float.
  # Lower score = higher effective priority (ZPOPMIN dequeues lowest score first)
  # For equal priority, earlier jobs (lower timestamp) have lower score -> FIFO within tier.
  # Aging: NORMAL jobs' effective priority increases as their timestamp ages.

  async def enqueue_job(self, job_id: uuid.UUID, priority_score: float | None = None) -> None:
    """
    Add a job to the priority queue.
    Score should be computed by the caller using compute_priority_score().
    Falls back to timestamp-only (NORMAL priority) if no score provided.
    """

    if priority_score is None:
      priority_score = time.time() # default: NORMAL, no priority boost
    await self.redis.zadd(REDIS_JOB_QUEUE, {str(job_id): priority_score})

  async def dequeue_job(self) -> str | None:
    """Pop the highest-priority job (lowest score) from the sorted set."""
    # ZPOPMIN returns [(member, score)] or empty list
    result = await self.redis.zpopmin(REDIS_JOB_QUEUE, count=1)
    if result:
      return result[0][0] # member string (job_id)
    return None
  
  async def queue_length(self) -> int:
    return await self.redis.zcard(REDIS_JOB_QUEUE)
  
  async def peek_queue(self, count: int = 10) -> list[str]:
    """Peek at the top N jobs by priority without removing them."""
    result = await self.redis.zrange(REDIS_JOB_QUEUE, 0, count - 1)
    return result
  
  async def remove_job(self, job_id: uuid.UUID) -> None:
    """Remove a specific job from the queue (e.g., on cancel)"""
    await self.redis.zrem(REDIS_JOB_QUEUE, str(job_id))

  # --- Worker Notifications (Pub/Sub) ---

  async def notify_workers(self, message: dict) -> None:
    """Publish a message to the worker notification channel."""
    await self.redis.publish(REDIS_WORKER_CHANNEL, json.dumps(message))

  async def subscribe_worker_channel(self):
    "Subscribe to worker notifications. Returns a pubsub object."
    pubsub = self.redis.pubsub()
    await pubsub.subscribe(REDIS_WORKER_CHANNEL)
    return pubsub
  
  # --- Job Locks (distributed mutual exclusion) ---
  # Don't need it because we use a different coordination model:
  #  only the leader dequeues from Redis (ZPOPMIN is atomic),
  #  assigns a worker, and sends a job_assigned message with a particular worker_id
  #
  #async def acquire_job_lock(self, job_id: uuid.UUID, holder: str, ttl: int = 60) -> bool:
  #  """Acquire a distributed lock for a specific job."""
  #  key = f"{REDIS_JOB_LOCK_PREFIX}{job_id}"
  #  return await self.redis.set(key, holder, nx=True, ex=ttl)

  #async def release_job_lock(self, job_id: uuid.UUID, holder: str) -> bool:
  #  """Release a job lock only if we hold it."""
  #  key = f"{REDIS_JOB_LOCK_PREFIX}{job_id}"
  #  script = """
  #  if redis.call("get", KEYS[1]) == ARGV[1] then
  #    return redis.call("del", KEYS[1])
  #  else
  #    return 0
  #  end
  #  """
  #  result = await self.redis.eval(script, 1, key, holder)
  #  return bool(result)
  

# Singleton instance
redis_queue = RedisQueue()

