"""Shared fixtures for evaluation tests against a live endure stack."""

from __future__ import annotations

import os

import httpx
import pytest

DEFAULT_API_URL = os.environ.get("ENDURE_API_URL", "http://localhost:8000")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "e2e: integration test requiring docker compose stack (api, scheduler, workers)",
    )
    config.addinivalue_line(
        "markers",
        "chaos: kills containers; set ENDURE_CHAOS=1 to enable; "
        "use ENDURE_WORKER_CONTAINER / ENDURE_SCHEDULER_CONTAINER to override targets",
    )
    config.addinivalue_line(
        "markers",
        "evaluate: evaluation chapter tests",
    )


@pytest.fixture(scope="session")
def api_url() -> str:
    return DEFAULT_API_URL.rstrip("/")


@pytest.fixture(scope="session")
def require_stack(api_url: str) -> str:
    """Skip the entire session if the endure API is not reachable."""
    try:
        r = httpx.get(f"{api_url}/api/v1/admin/health", timeout=5.0)
        r.raise_for_status()
    except Exception as exc:
        pytest.skip(f"Endure stack not running at {api_url}: {exc}")
    return api_url


@pytest.fixture
async def client(require_stack: str) -> httpx.AsyncClient:
    async with httpx.AsyncClient(base_url=require_stack, timeout=120.0) as c:
        yield c
