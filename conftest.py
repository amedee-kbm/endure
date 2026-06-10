import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

# os.stat('') raises FileNotFoundError on Windows; strip empty entries before
# importlib.metadata scans sys.path for installed packages.
sys.path[:] = [p for p in sys.path if p]

import pytest


@pytest.fixture
def mock_checkpoint_manager():
    mock = MagicMock()
    mock.save_checkpoint = AsyncMock(return_value=MagicMock())
    mock.load_latest_checkpoint = AsyncMock(return_value=None)
    mock.save_job_state_snapshot = AsyncMock(return_value=None)
    mock.cleanup_checkpoints = AsyncMock(return_value=0)
    with (
        patch("src.worker.executor.checkpoint_manager", mock),
        patch("src.worker.isolation.checkpoint_manager", mock),
    ):
        yield mock


@pytest.fixture
def job_id():
    return uuid.uuid4()
