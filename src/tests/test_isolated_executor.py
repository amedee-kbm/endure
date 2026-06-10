import asyncio
import json
import multiprocessing
import uuid
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.worker.isolation import IsolatedExecutor, _run_job_in_process_v2
from src.worker.pipe_protocol import build_checkpoint_message, build_result_message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_pipe(conn):
    """Read all messages from a pipe connection until EOF."""
    messages = []
    try:
        while True:
            messages.append(conn.recv())
    except EOFError:
        pass
    return messages


# ---------------------------------------------------------------------------
# Round 2: Child-side subprocess runner
# ---------------------------------------------------------------------------


class TestIsolatedExecutorSubprocessRunner:
    def test_child_sends_typed_result_on_success(self):
        parent, child = multiprocessing.Pipe()
        _run_job_in_process_v2(
            job_type="src.tests.fixtures.jobs:SuccessJob",
            payload_json=json.dumps({"x": 3}),
            resume_state_json=None,
            memory_mb=512,
            cpu_seconds=3600,
            result_pipe=child,
            checkpoint_pipe=None,
        )
        child.close()

        msg = parent.recv()
        parent.close()

        assert msg["type"] == "result"
        assert msg["success"] is True
        assert msg["result"] == {"output": 6}

    def test_child_sends_failure_on_exception(self):
        parent, child = multiprocessing.Pipe()
        _run_job_in_process_v2(
            job_type="src.tests.fixtures.jobs:FailingJob",
            payload_json=json.dumps({}),
            resume_state_json=None,
            memory_mb=512,
            cpu_seconds=3600,
            result_pipe=child,
            checkpoint_pipe=None,
        )
        child.close()

        msg = parent.recv()
        parent.close()

        assert msg["type"] == "result"
        assert msg["success"] is False
        assert "intentional" in msg["error"].lower()

    def test_child_sends_checkpoint_messages_then_result(self):
        result_parent, result_child = multiprocessing.Pipe()
        ckpt_parent, ckpt_child = multiprocessing.Pipe()

        _run_job_in_process_v2(
            job_type="src.tests.fixtures.jobs:CheckpointingJob",
            payload_json=json.dumps({"stages": 2}),
            resume_state_json=None,
            memory_mb=512,
            cpu_seconds=3600,
            result_pipe=result_child,
            checkpoint_pipe=ckpt_child,
        )
        result_child.close()
        ckpt_child.close()

        checkpoints = _read_pipe(ckpt_parent)
        result_msg = result_parent.recv()
        result_parent.close()
        ckpt_parent.close()

        assert len(checkpoints) == 2
        assert all(m["type"] == "checkpoint" for m in checkpoints)
        assert checkpoints[0]["sequence"] == 1
        assert checkpoints[1]["sequence"] == 2
        assert result_msg["type"] == "result"
        assert result_msg["success"] is True

    def test_child_sends_result_only_when_no_checkpoint_pipe(self):
        """CheckpointingJob with checkpoint_pipe=None should still return a result."""
        parent, child = multiprocessing.Pipe()
        _run_job_in_process_v2(
            job_type="src.tests.fixtures.jobs:CheckpointingJob",
            payload_json=json.dumps({"stages": 2}),
            resume_state_json=None,
            memory_mb=512,
            cpu_seconds=3600,
            result_pipe=child,
            checkpoint_pipe=None,
        )
        child.close()
        msg = parent.recv()
        parent.close()
        assert msg["type"] == "result"
        assert msg["success"] is True

    def test_child_passes_resume_state_to_job(self):
        """resume_state_json is deserialized and passed to job.run()."""
        parent, child = multiprocessing.Pipe()
        resume = json.dumps({"previous": True})
        _run_job_in_process_v2(
            job_type="src.tests.fixtures.jobs:SuccessJob",
            payload_json=json.dumps({"x": 1}),
            resume_state_json=resume,
            memory_mb=512,
            cpu_seconds=3600,
            result_pipe=child,
            checkpoint_pipe=None,
        )
        child.close()
        msg = parent.recv()
        parent.close()
        # SuccessJob ignores resume_state but must not crash on it
        assert msg["success"] is True


# ---------------------------------------------------------------------------
# Round 3: Parent-side message pump
# ---------------------------------------------------------------------------


@pytest.fixture
def executor():
    return IsolatedExecutor()


class TestIsolatedExecutorMessagePump:
    async def test_result_only_no_checkpoint_save(
        self, executor, job_id, mock_checkpoint_manager
    ):
        result_msg = build_result_message(success=True, result={"x": 1})
        queue = asyncio.Queue()
        await queue.put(result_msg)

        async def fake_read(conn):
            return await queue.get()

        with (
            patch.object(executor, "_read_one_message", side_effect=fake_read),
            patch.object(
                executor, "_spawn_process", return_value=(MagicMock(), MagicMock())
            ),
        ):
            result = await executor._pump_messages(
                parent_conn=MagicMock(),
                process=MagicMock(),
                job_id=job_id,
                timeout_seconds=5,
            )

        assert result["success"] is True
        assert result["result"] == {"x": 1}
        mock_checkpoint_manager.save_checkpoint.assert_not_called()

    async def test_checkpoint_message_triggers_save(
        self, executor, job_id, mock_checkpoint_manager
    ):
        seq, data = 1, b"checkpoint-state"
        queue = asyncio.Queue()
        await queue.put(build_checkpoint_message(sequence=seq, data=data))
        await queue.put(build_result_message(success=True, result={}))

        async def fake_read(conn):
            return await queue.get()

        with (
            patch.object(executor, "_read_one_message", side_effect=fake_read),
            patch.object(
                executor, "_spawn_process", return_value=(MagicMock(), MagicMock())
            ),
        ):
            result = await executor._pump_messages(
                parent_conn=MagicMock(),
                process=MagicMock(),
                job_id=job_id,
                timeout_seconds=5,
            )

        assert result["success"] is True
        mock_checkpoint_manager.save_checkpoint.assert_awaited_once_with(
            job_id, seq, data
        )

    async def test_multiple_checkpoints_saved_in_order(
        self, executor, job_id, mock_checkpoint_manager
    ):
        queue = asyncio.Queue()
        await queue.put(build_checkpoint_message(sequence=1, data=b"state-1"))
        await queue.put(build_checkpoint_message(sequence=2, data=b"state-2"))
        await queue.put(build_result_message(success=True, result={}))

        async def fake_read(conn):
            return await queue.get()

        with (
            patch.object(executor, "_read_one_message", side_effect=fake_read),
            patch.object(
                executor, "_spawn_process", return_value=(MagicMock(), MagicMock())
            ),
        ):
            await executor._pump_messages(
                parent_conn=MagicMock(),
                process=MagicMock(),
                job_id=job_id,
                timeout_seconds=5,
            )

        assert mock_checkpoint_manager.save_checkpoint.await_count == 2
        calls = mock_checkpoint_manager.save_checkpoint.await_args_list
        assert calls[0] == call(job_id, 1, b"state-1")
        assert calls[1] == call(job_id, 2, b"state-2")

    async def test_timeout_kills_process_returns_failure(
        self, executor, job_id, mock_checkpoint_manager
    ):
        async def hanging_read(conn):
            await asyncio.sleep(9999)

        mock_process = MagicMock()
        mock_process.is_alive.return_value = True

        with (
            patch.object(executor, "_read_one_message", side_effect=hanging_read),
            patch.object(
                executor, "_spawn_process", return_value=(mock_process, MagicMock())
            ),
        ):
            result = await executor._pump_messages(
                parent_conn=MagicMock(),
                process=mock_process,
                job_id=job_id,
                timeout_seconds=0.01,
            )

        assert result["success"] is False
        assert "timed out" in result["error"].lower()
        mock_process.kill.assert_called_once()

    async def test_eof_returns_failure_with_exit_code(
        self, executor, job_id, mock_checkpoint_manager
    ):
        async def crashing_read(conn):
            raise EOFError

        mock_process = MagicMock()
        mock_process.exitcode = -9

        with (
            patch.object(executor, "_read_one_message", side_effect=crashing_read),
            patch.object(
                executor, "_spawn_process", return_value=(mock_process, MagicMock())
            ),
        ):
            result = await executor._pump_messages(
                parent_conn=MagicMock(),
                process=mock_process,
                job_id=job_id,
                timeout_seconds=5,
            )

        assert result["success"] is False
        assert "crashed" in result["error"].lower()

    async def test_checkpoint_save_failure_is_non_fatal(
        self, executor, job_id, mock_checkpoint_manager
    ):
        mock_checkpoint_manager.save_checkpoint.side_effect = Exception("DB down")
        queue = asyncio.Queue()
        await queue.put(build_checkpoint_message(sequence=1, data=b"state"))
        await queue.put(build_result_message(success=True, result={"ok": True}))

        async def fake_read(conn):
            return await queue.get()

        with (
            patch.object(executor, "_read_one_message", side_effect=fake_read),
            patch.object(
                executor, "_spawn_process", return_value=(MagicMock(), MagicMock())
            ),
        ):
            result = await executor._pump_messages(
                parent_conn=MagicMock(),
                process=MagicMock(),
                job_id=job_id,
                timeout_seconds=5,
            )

        assert result["success"] is True

    async def test_checkpoint_without_job_id_is_dropped(
        self, executor, mock_checkpoint_manager
    ):
        queue = asyncio.Queue()
        await queue.put(build_checkpoint_message(sequence=1, data=b"state"))
        await queue.put(build_result_message(success=True, result={}))

        async def fake_read(conn):
            return await queue.get()

        with (
            patch.object(executor, "_read_one_message", side_effect=fake_read),
            patch.object(
                executor, "_spawn_process", return_value=(MagicMock(), MagicMock())
            ),
        ):
            result = await executor._pump_messages(
                parent_conn=MagicMock(),
                process=MagicMock(),
                job_id=None,
                timeout_seconds=5,
            )

        mock_checkpoint_manager.save_checkpoint.assert_not_called()
        assert result["success"] is True
