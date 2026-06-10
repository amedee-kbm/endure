import logging
from abc import ABC, abstractmethod
from pathlib import Path

from django.conf import settings

logger = logging.getLogger("endure.checkpoint.store")


class CheckpointStore(ABC):
    @abstractmethod
    async def save(self, job_id: str, sequence: int, data: bytes) -> str:
        """Save checkpoint data. Returns the storage path."""
        ...

    @abstractmethod
    async def load(self, storage_path: str) -> bytes:
        """Load checkpoint data from path."""
        ...

    @abstractmethod
    async def delete(self, storage_path: str) -> None:
        """Delete a checkpoint file."""
        ...

    @abstractmethod
    async def list_checkpoints(self, job_id: str) -> list[str]:
        """List all checkpoint paths for a job."""
        ...


class LocalCheckpointStore(CheckpointStore):
    def __init__(self, base_dir: str | None = None):
        self.base_dir = Path(base_dir or settings.CHECKPOINT_DIR)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _job_dir(self, job_id: str) -> Path:
        return self.base_dir / job_id

    def _checkpoint_path(self, job_id: str, sequence: int) -> Path:
        return self._job_dir(job_id) / f"checkpoint_{sequence:06d}.bin"

    async def save(self, job_id: str, sequence: int, data: bytes) -> str:
        job_dir = self._job_dir(job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        path = self._checkpoint_path(job_id, sequence)
        path.write_bytes(data)
        logger.info(f"Checkpoint saved: {path} ({len(data)} bytes)")
        return str(path)

    async def load(self, storage_path: str) -> bytes:
        path = Path(storage_path)
        if not path.exists():
            raise FileNotFoundError(f"Checkpoint not found: {storage_path}")
        data = path.read_bytes()
        logger.info(f"Checkpoint loaded: {storage_path} ({len(data)} bytes)")
        return data

    async def delete(self, storage_path: str) -> None:
        path = Path(storage_path)
        if path.exists():
            path.unlink()
            logger.info(f"Checkpoint deleted: {storage_path}")

    async def list_checkpoints(self, job_id: str) -> list[str]:
        job_dir = self._job_dir(job_id)
        if not job_dir.exists():
            return []
        return sorted(
            str(p) for p in job_dir.iterdir()
            if p.name.startswith("checkpoint_") and p.suffix == ".bin"
        )

    async def cleanup_job(self, job_id: str) -> int:
        """Delete all checkpoints for a job. Returns number of files deleted."""
        paths = await self.list_checkpoints(job_id)
        for path in paths:
            await self.delete(path)
        job_dir = self._job_dir(job_id)
        if job_dir.exists() and not any(job_dir.iterdir()):
            job_dir.rmdir()
        return len(paths)