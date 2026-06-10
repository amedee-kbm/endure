"""
Job executor — runs jobs in-process with checkpoint support.
"""

import importlib
import logging
import traceback
import uuid

from src.checkpoint.manager import checkpoint_manager
from src.framework.context import _current_job_id, _step_counter

logger = logging.getLogger("endure.worker.executor")


class JobExecutor:
    async def execute(
        self,
        job_type: str,
        payload: dict,
        job_id: uuid.UUID | None = None,
        timeout_seconds: int | None = None,
    ) -> dict:
        try:
            logger.info(
                f"Starting execution of job_type: '{job_type}' for job_id: {job_id}"
            )
            if ":" not in job_type:
                raise ValueError(
                    f"Invalid job_type '{job_type}'. Expected format 'module.path:ClassName'"
                )

            module_path, class_name = job_type.rsplit(":", 1)
            module = importlib.import_module(module_path)
            job_class = getattr(module, class_name)
            job_instance = job_class()

            disable_ckpt = bool(payload.get("disable_checkpointing"))
            use_checkpointing = (
                job_id is not None
                and job_instance.supports_checkpointing()
                and not disable_ckpt
            )

            resume_state = None
            if use_checkpointing:
                assert job_id is not None
                try:
                    checkpoint_data = await checkpoint_manager.load_latest_checkpoint(job_id)
                    if checkpoint_data:
                        if (
                            isinstance(checkpoint_data, (tuple, list))
                            and len(checkpoint_data) == 2
                        ):
                            seq, data = checkpoint_data
                            resume_state = job_instance.parse_checkpoint_data(data)
                            job_instance._checkpoint_sequence = seq
                            logger.info(
                                f"Resuming job {job_id} from checkpoint seq={seq}"
                            )
                            completed = resume_state.get("completed_stages", [])
                            if completed:
                                from src.services.event_logger import record_event

                                await record_event(
                                    job_id,
                                    "RUNNING",
                                    detail=(
                                        f"Skipping {len(completed)} completed stage(s) "
                                        f"({', '.join(completed)})"
                                    ),
                                )
                        else:
                            logger.warning(
                                f"Invalid checkpoint data format for job {job_id}"
                            )
                except Exception as e:
                    logger.error(f"Failed to load checkpoint for job {job_id}: {e}")

            tok_job = _current_job_id.set(job_id) if job_id is not None else None
            tok_counter = _step_counter.set(0)
            try:
                if use_checkpointing:
                    assert job_id is not None
                    result = await self._execute_with_checkpointing(
                        job_instance, payload, job_id, resume_state
                    )
                else:
                    result = await job_instance.run(payload, resume_state=resume_state)
            finally:
                if tok_job is not None:
                    _current_job_id.reset(tok_job)
                _step_counter.reset(tok_counter)

            return {"success": True, "result": result}

        except Exception as e:
            logger.exception(f"Job execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "traceback": traceback.format_exc(),
            }

    async def _execute_with_checkpointing(
        self,
        job_instance,
        payload: dict,
        job_id: uuid.UUID,
        resume_state: dict | None,
    ) -> dict:
        async def ckpt_callback(sequence: int, data: bytes) -> None:
            try:
                await checkpoint_manager.save_checkpoint(job_id, sequence, data)
                await checkpoint_manager.cleanup_checkpoints(job_id, keep_latest=2)
            except Exception:
                logger.exception(f"Checkpoint save failed for job {job_id}")

        return await job_instance.run(
            payload, resume_state=resume_state, checkpoint_callback=ckpt_callback
        )
