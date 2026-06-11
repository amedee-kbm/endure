"""Session-scoped fixtures shared across all evaluate tests."""

import pytest

from src.evaluate.helpers import ensure_tenant

# ---------------------------------------------------------------------------
# Tenant
# ---------------------------------------------------------------------------

TENANT_NAME = "evaluate"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "demonstration: functional correctness tests (D1-D4); assert pass/fail",
    )
    config.addinivalue_line(
        "markers",
        "experiment: measurement runs (E1-E5); produce JSON/CSV in loadtest-results/",
    )


@pytest.fixture(scope="session")
def tenant():
    return ensure_tenant(TENANT_NAME)


@pytest.fixture(scope="session")
def tenant_id(tenant) -> str:
    return str(tenant["id"])
