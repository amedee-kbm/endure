"""
Leader election using a PostgreSQL row lease.

We keep a singleton row in the `scheduler_leader` table and treat it as a
time-based lease. A scheduler becomes leader if it can atomically claim the
lease, and must periodically renew it.

Aside: this replaces the advisory-lock approach (which relied on holding a
single DB session open).
"""

import logging
import uuid
from datetime import datetime, timezone

from django.conf import settings
from django.db.models import Q

from src.models import SchedulerLeader

logger = logging.getLogger("src.scheduler.leader")

LEADER_SINGLETON_ID = 1


class LeaderElection:
    def __init__(self, instance_id: str | None = None):
        self.instance_id = (
            instance_id
            or getattr(settings, "SCHEDULER_INSTANCE_ID", "")
            or str(uuid.uuid4())[:8]
        )
        self.is_leader = False

    async def try_acquire(self) -> bool:
        """Try to acquire leadership (non-blocking)."""
        now = datetime.now(timezone.utc)
        cutoff_dt = datetime.fromtimestamp(
            now.timestamp() - settings.LEADER_LOCK_TTL, tz=timezone.utc
        )

        existing = await SchedulerLeader.objects.filter(id=LEADER_SINGLETON_ID).afirst()
        if not existing:
            try:
                await SchedulerLeader.objects.acreate(
                    id=LEADER_SINGLETON_ID,
                    holder_id=self.instance_id,
                    acquired_at=now,
                    renewed_at=now,
                )
                self.is_leader = True
                logger.info(
                    f"Instance {self.instance_id} acquired scheduler leadership."
                )
                return True
            except Exception:
                self.is_leader = False
                return False

        updated = (
            await SchedulerLeader.objects.filter(id=LEADER_SINGLETON_ID)
            .filter(
                Q(holder_id=self.instance_id)
                | Q(
                    renewed_at__lt=cutoff_dt
                )  # this instance holds it or it was last renewed before cutoff_dt
            )
            .aupdate(holder_id=self.instance_id, acquired_at=now, renewed_at=now)
        )

        self.is_leader = updated == 1
        if self.is_leader:
            logger.info(f"Instance {self.instance_id} acquired scheduler leadership.")
        return self.is_leader

    async def renew_heartbeat(self) -> bool:
        """Renew the lease. Returns False if we've lost leadership."""
        if not self.is_leader:
            return False

        now = datetime.now(timezone.utc)
        updated = await SchedulerLeader.objects.filter(
            id=LEADER_SINGLETON_ID, holder_id=self.instance_id
        ).aupdate(renewed_at=now)

        if updated == 1:
            return True

        self.is_leader = False
        logger.warning(f"Instance {self.instance_id} lost leadership!")
        return False

    async def get_current_leader(self) -> dict | None:
        """Get info about the current leader (for health checks)."""
        row = await SchedulerLeader.objects.filter(id=LEADER_SINGLETON_ID).afirst()
        if not row:
            return None

        return {
            "holder_id": row.holder_id,
            "acquired_at": row.acquired_at.isoformat() if row.acquired_at else None,
            "renewed_at": row.renewed_at.isoformat() if row.renewed_at else None,
        }
