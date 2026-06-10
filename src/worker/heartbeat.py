"""
Worker heartbeat sender.
Periodically updates the worker's last_heartbeat timestamp in PostgreSQL.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from django.conf import settings

from src.constants import WorkerState
from src.models import Worker

logger = logging.getLogger("endure.worker.heartbeat")


class HeartbeatSender:
    def __init__(
        self,
        worker_id: uuid.UUID,
        active_jobs: dict[uuid.UUID, asyncio.Task] | None = None,
    ):
        self.worker_id = worker_id
        # Shared reference to WorkerNode._active_jobs; mutated by the worker loop.
        self._active_jobs = active_jobs
        self._running = False

    async def start(self):
        """Send heartbeats on a loop."""
        self._running = True
        logger.info(f"Heartbeat sender started for worker {self.worker_id}")

        while self._running:
            try:
                worker = await Worker.objects.filter(id=self.worker_id).afirst()
                if worker:
                    worker.last_heartbeat = datetime.now(timezone.utc)
                    if worker.state == WorkerState.OFFLINE:
                        # Fix 4 (bookkeeping) — before going ONLINE again:
                        # 1. Cancel all in-flight asyncio tasks so the execution
                        #    paths hit their CancelledException at the next await.
                        #    This is best-effort; the ownership-gated CAS in
                        #    _execute_job (Fix 1/3) is the primary correctness guard.
                        # 2. Reset the in-flight counters so the scheduler sees
                        #    accurate capacity after the worker rejoins.
                        if self._active_jobs:
                            for jid, task in list(self._active_jobs.items()):
                                task.cancel()
                                logger.info(
                                    f"Cancelled in-flight job {jid} "
                                    f"(worker {self.worker_id} self-detected OFFLINE)"
                                )
                        worker.inflight_job_count = 0
                        worker.tenant_inflight_job_count_map = {}
                        worker.state = WorkerState.ONLINE
                        logger.info(
                            f"Worker {self.worker_id} recovered from OFFLINE: "
                            f"cancelled in-flight tasks, reset inflight_count=0"
                        )
                    await worker.asave(
                        update_fields=[
                            "last_heartbeat",
                            "state",
                            "inflight_job_count",
                            "tenant_inflight_job_count_map",
                        ]
                    )
            except Exception:
                logger.exception("Heartbeat send failed")

            await asyncio.sleep(settings.WORKER_HEARTBEAT_INTERVAL)

    def stop(self):
        self._running = False
