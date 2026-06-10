"""
Process-level resource isolation for job execution.
Runs jobs in subprocesses with CPU, memory, and timeout limits.
Uses resource module on Linux/macOS to enforce limits (no-op on Windows).

Checkpoint data flows back from the child through a separate pipe so the
parent event loop can persist it via checkpoint_manager without the child
needing a Django setup.
"""

import asyncio
import json
import logging
import multiprocessing
import sys
import traceback
import uuid

from src.checkpoint.manager import checkpoint_manager
from src.worker.pipe_protocol import (
    CheckpointMessage,
    ResultMessage,
    build_checkpoint_message,
    build_result_message,
    parse_pipe_message,
)

logger = logging.getLogger("endure.worker.isolation")

DEFAULT_MEMORY_LIMIT_MB = 512
DEFAULT_CPU_TIME_LIMIT = 3600  # seconds


def _set_resource_limits(memory_mb: int, cpu_seconds: int):
    """Set resource limits inside the subprocess. No-op on Windows."""
    if sys.platform == "win32":
        return

    import resource

    memory_bytes = memory_mb * 1024 * 1024
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    except ValueError:
        try:
            resource.setrlimit(resource.RLIMIT_RSS, (memory_bytes, memory_bytes))
        except (ValueError, AttributeError):
            pass

    try:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
    except ValueError:
        pass


def _run_job_in_process_v2(
    job_type: str,
    payload_json: str,
    resume_state_json: str | None,
    memory_mb: int,
    cpu_seconds: int,
    result_pipe,
    checkpoint_pipe,
):
    """
    Child-side entry point. Imports and runs the job, sends typed pipe messages:
      - checkpoint messages through checkpoint_pipe (if provided)
      - final result message through result_pipe
    """
    import asyncio
    import importlib

    _set_resource_limits(memory_mb, cpu_seconds)

    async def _execute():
        try:
            module_path, class_name = job_type.rsplit(":", 1)
            module = importlib.import_module(module_path)
            job_class = getattr(module, class_name)
            job_instance = job_class()

            payload = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
            resume_state = json.loads(resume_state_json) if resume_state_json else None

            # Determine how to call run() — with or without checkpoint_callback
            supports_ckpt = hasattr(job_instance, "supports_checkpointing") and job_instance.supports_checkpointing()
            if supports_ckpt and checkpoint_pipe is not None:
                async def _ckpt_callback(sequence: int, data: bytes):
                    checkpoint_pipe.send(build_checkpoint_message(sequence=sequence, data=data))

                result = await job_instance.run(
                    payload,
                    resume_state=resume_state,
                    checkpoint_callback=_ckpt_callback,
                )
            else:
                result = await job_instance.run(payload, resume_state=resume_state)

            return build_result_message(success=True, result=result)

        except Exception as e:
            return build_result_message(
                success=False,
                error=str(e),
                traceback_str=traceback.format_exc(),
            )

    try:
        msg = asyncio.run(_execute())
        result_pipe.send(msg)
    except Exception as e:
        result_pipe.send(
            build_result_message(
                success=False,
                error=str(e),
                traceback_str=traceback.format_exc(),
            )
        )
    finally:
        result_pipe.close()
        if checkpoint_pipe is not None:
            checkpoint_pipe.close()


class IsolatedExecutor:
    """Execute jobs in isolated subprocesses with resource limits and checkpoint pipe support."""

    def _spawn_process(
        self,
        job_type: str,
        payload_json: str,
        resume_json: str | None,
        memory_mb: int,
        cpu_seconds: int,
    ) -> tuple[multiprocessing.Process, object, object]:
        """
        Spawn a subprocess. Returns (process, result_parent_conn, checkpoint_parent_conn).
        Child ends of both pipes are closed in the parent before returning.
        """
        result_parent, result_child = multiprocessing.Pipe(duplex=False)
        ckpt_parent, ckpt_child = multiprocessing.Pipe(duplex=False)

        process = multiprocessing.Process(
            target=_run_job_in_process_v2,
            args=(
                job_type,
                payload_json,
                resume_json,
                memory_mb,
                cpu_seconds,
                result_child,
                ckpt_child,
            ),
        )
        process.start()
        result_child.close()
        ckpt_child.close()

        return process, result_parent, ckpt_parent

    async def _read_one_message(self, conn) -> dict:
        """Read one message from a pipe connection without blocking the event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, conn.recv)

    async def _drain_checkpoints(
        self,
        ckpt_conn,
        job_id: uuid.UUID | None,
    ):
        """
        Drain all pending checkpoint messages from ckpt_conn until EOF.
        Persists each one via checkpoint_manager (non-fatal on error).
        Runs as a background task alongside the result waiter.
        """
        try:
            while True:
                raw = await self._read_one_message(ckpt_conn)
                msg = parse_pipe_message(raw)
                if isinstance(msg, CheckpointMessage) and job_id is not None:
                    try:
                        await checkpoint_manager.save_checkpoint(
                            job_id, msg.sequence, msg.data
                        )
                    except Exception:
                        logger.exception(
                            f"Failed to persist checkpoint seq={msg.sequence} for job {job_id}"
                        )
        except EOFError:
            pass  # child closed the pipe — normal end of stream

    async def _pump_messages(
        self,
        parent_conn,
        process: multiprocessing.Process,
        job_id: uuid.UUID | None,
        timeout_seconds: float,
    ) -> dict:
        """
        Drive the message pump: drain checkpoint pipe in background, wait for
        the result message, enforce overall timeout.
        """
        # _spawn_process returns (process, result_conn, ckpt_conn) but callers
        # may pass just parent_conn for the result pipe in tests.  The ckpt pipe
        # is stored on the executor when _spawn_process is used internally.
        ckpt_conn = getattr(self, "_ckpt_conn", None)

        ckpt_task = None
        if ckpt_conn is not None:
            ckpt_task = asyncio.create_task(
                self._drain_checkpoints(ckpt_conn, job_id)
            )

        _terminated = False
        try:
            async with asyncio.timeout(timeout_seconds):
                while True:
                    raw = await self._read_one_message(parent_conn)
                    msg = parse_pipe_message(raw)

                    if isinstance(msg, CheckpointMessage):
                        # Fallback: handle inline checkpoint messages on the result pipe
                        # (used in tests that don't go through _spawn_process)
                        if job_id is not None:
                            try:
                                await checkpoint_manager.save_checkpoint(
                                    job_id, msg.sequence, msg.data
                                )
                            except Exception:
                                logger.exception(
                                    f"Checkpoint save failed for job {job_id}"
                                )
                    elif isinstance(msg, ResultMessage):
                        return {
                            "success": msg.success,
                            "result": msg.result,
                            "error": msg.error,
                            "traceback": msg.traceback,
                        }

        except TimeoutError:
            logger.warning(
                f"Job {job_id} timed out after {timeout_seconds}s, killing process"
            )
            _terminated = True
            if process.is_alive():
                process.kill()
            return {
                "success": False,
                "error": f"Process timed out after {timeout_seconds}s",
            }

        except EOFError:
            return {
                "success": False,
                "error": f"Process crashed (exit code: {process.exitcode})",
            }

        finally:
            if ckpt_task is not None:
                ckpt_task.cancel()
                try:
                    await ckpt_task
                except (asyncio.CancelledError, Exception):
                    pass
            if not _terminated and process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()

    async def execute(
        self,
        job_type: str,
        payload: dict,
        job_id: uuid.UUID | None = None,
        resume_state: dict | None = None,
        timeout_seconds: int = DEFAULT_CPU_TIME_LIMIT,
        memory_limit_mb: int = DEFAULT_MEMORY_LIMIT_MB,
        cpu_limit_seconds: int = DEFAULT_CPU_TIME_LIMIT,
    ) -> dict:
        """
        Run a job in a subprocess with resource limits and checkpoint pipe support.
        """
        payload_json = json.dumps(payload)
        resume_json = json.dumps(resume_state) if resume_state else None

        process, result_conn, ckpt_conn = self._spawn_process(
            job_type, payload_json, resume_json, memory_limit_mb, cpu_limit_seconds
        )
        self._ckpt_conn = ckpt_conn

        try:
            return await self._pump_messages(
                parent_conn=result_conn,
                process=process,
                job_id=job_id,
                timeout_seconds=timeout_seconds,
            )
        finally:
            result_conn.close()
            ckpt_conn.close()
            self._ckpt_conn = None
