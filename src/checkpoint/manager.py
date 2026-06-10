"""
Checkpoint manager — stores blobs directly in PostgreSQL.
"""

import logging
import uuid

from src.models import Checkpoint, Job

logger = logging.getLogger("endure.checkpoint.manager")


class CheckpointManager:
    async def save_checkpoint(
        self, job_id: uuid.UUID, sequence: int, data: bytes
    ) -> Checkpoint:
        checkpoint = await Checkpoint.objects.acreate(
            job_id=job_id,
            sequence_number=sequence,
            data=data,
            size_bytes=len(data),
        )
        logger.info(
            f"Checkpoint saved for job {job_id}: seq={sequence}, size={len(data)}"
        )
        return checkpoint

    async def load_latest_checkpoint(
        self, job_id: uuid.UUID
    ) -> tuple[int, bytes] | None:
        try:
            job = await Job.objects.aget(id=job_id)
        except Job.DoesNotExist:
            return None

        checkpoint = await job.get_latest_checkpoint()
        if checkpoint is None:
            return None

        logger.info(
            f"Loaded checkpoint for job {job_id}: seq={checkpoint.sequence_number}, "
            f"size={checkpoint.size_bytes}"
        )
        return checkpoint.sequence_number, bytes(checkpoint.data)

    async def save_job_state_snapshot(self, job_id: uuid.UUID, job_instance) -> None:
        if not hasattr(job_instance, "_checkpoint_sequence"):
            job_instance._checkpoint_sequence = 0
        state = await job_instance.save_state()
        data = job_instance.get_checkpoint_data(state)
        job_instance._checkpoint_sequence += 1
        await self.save_checkpoint(job_id, job_instance._checkpoint_sequence, data)
        await self.cleanup_checkpoints(job_id, keep_latest=2)

    async def cleanup_checkpoints(self, job_id: uuid.UUID, keep_latest: int = 1) -> int:
        all_checkpoints = [
            cp
            async for cp in Checkpoint.objects.filter(job_id=job_id).order_by(
                "-sequence_number"
            )
        ]

        if len(all_checkpoints) <= keep_latest:
            return 0

        to_delete = all_checkpoints[keep_latest:]
        deleted = 0
        for cp in to_delete:
            await cp.adelete()
            deleted += 1

        logger.info(f"Cleaned up {deleted} old checkpoints for job {job_id}")
        return deleted


# Singleton
checkpoint_manager = CheckpointManager()
