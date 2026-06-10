import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from django.test import override_settings

from src.worker.executor import JobExecutor


@pytest.fixture
def executor():
    return JobExecutor()


class TestJobExecutorIsolationRouting:
    @override_settings(USE_PROCESS_ISOLATION=False)
    async def test_in_process_when_isolation_disabled(
        self, executor, job_id, mock_checkpoint_manager
    ):
        mock_result = {"success": True, "result": {"out": 1}}

        with (
            patch.object(
                executor, "_execute_in_process", new_callable=AsyncMock, return_value=mock_result
            ) as mock_in_proc,
            patch("src.worker.executor.IsolatedExecutor") as mock_iso_cls,
        ):
            result = await executor.execute(
                "src.tests.fixtures.jobs:SuccessJob",
                payload={},
                job_id=job_id,
            )

        mock_in_proc.assert_awaited_once()
        mock_iso_cls.assert_not_called()
        assert result["success"] is True

    @override_settings(USE_PROCESS_ISOLATION=True)
    async def test_isolated_when_isolation_enabled(
        self, executor, job_id, mock_checkpoint_manager
    ):
        mock_result = {"success": True, "result": {}}

        with patch.object(
            executor, "_execute_isolated", new_callable=AsyncMock, return_value=mock_result
        ) as mock_iso:
            result = await executor.execute(
                "src.tests.fixtures.jobs:SuccessJob",
                payload={},
                job_id=job_id,
            )

        mock_iso.assert_awaited_once()
        assert result["success"] is True

    @override_settings(USE_PROCESS_ISOLATION=True)
    async def test_timeout_forwarded_to_isolated_executor(
        self, executor, job_id, mock_checkpoint_manager
    ):
        with patch.object(
            executor, "_execute_isolated", new_callable=AsyncMock,
            return_value={"success": True, "result": {}}
        ) as mock_iso:
            await executor.execute(
                "src.tests.fixtures.jobs:SuccessJob",
                payload={},
                job_id=job_id,
                timeout_seconds=120,
            )

        _, kwargs = mock_iso.call_args
        assert kwargs.get("timeout_seconds") == 120

    @override_settings(USE_PROCESS_ISOLATION=False)
    async def test_in_process_result_returned_unchanged(
        self, executor, job_id, mock_checkpoint_manager
    ):
        expected = {"success": True, "result": {"value": 99}}

        with patch.object(
            executor, "_execute_in_process", new_callable=AsyncMock, return_value=expected
        ):
            result = await executor.execute(
                "src.tests.fixtures.jobs:SuccessJob",
                payload={},
                job_id=job_id,
            )

        assert result == expected
