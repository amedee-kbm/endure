"""
Reporting E2E tests — §4.2 Functional Validation.

Tests DailyImportJob end-to-end: completion, data quality threshold, and
step-outputs population.  Requires a running Docker Compose stack.
"""

import pytest

from src.evaluate.helpers import (
    create_tenant,
    get_events,
    submit_job,
    unique_name,
    wait_for_job,
)

pytestmark = [pytest.mark.e2e, pytest.mark.evaluate, pytest.mark.asyncio]

DAILY_IMPORT = "src.reporting.jobs.daily_import:DailyImportJob"


async def _submit_report(client, *, tenant_id: str, payload: dict) -> str:
    """Submit via the /reports convenience endpoint; return job_id."""
    r = await client.post(
        "/api/v1/reports",
        json={
            "tenant_id": tenant_id,
            "report_type": "daily_import",
            "payload": payload,
            "max_retries": 0,
            "timeout_seconds": 180,
        },
    )
    r.raise_for_status()
    return str(r.json()["job_id"])


async def _get_report(client, job_id: str) -> dict:
    r = await client.get(f"/api/v1/reports/{job_id}")
    r.raise_for_status()
    return r.json()


async def _get_step_outputs(client, job_id: str) -> dict:
    r = await client.get(f"/api/v1/jobs/{job_id}/step-outputs")
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# End-to-end completion
# ---------------------------------------------------------------------------


async def test_daily_import_completes(client):
    tenant_id = await create_tenant(client, name=unique_name("rpt-import"))

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        payload={"date": "2026-06-01", "n_files": 5, "rows_per_file": 100, "seed": 1},
    )

    job = await wait_for_job(client, job_id, timeout=120.0)
    assert job["state"] == "COMPLETED", f"Expected COMPLETED, got {job['state']}"

    report = await _get_report(client, job_id)
    assert report["artifact_path"] is not None, "Expected artifact_path in result"
    assert report["artifact_path"].endswith(".xlsx"), (
        f"artifact_path should end in .xlsx, got {report['artifact_path']}"
    )

    events = await get_events(client, job_id)
    event_types = [e["event"] for e in events]
    assert "QUEUED" in event_types
    assert "RUNNING" in event_types
    assert "COMPLETED" in event_types


# ---------------------------------------------------------------------------
# Data quality threshold exceeded → job fails
# ---------------------------------------------------------------------------


async def test_data_quality_threshold_exceeded(client):
    """inject_errors far beyond 10% threshold causes job to fail."""
    tenant_id = await create_tenant(client, name=unique_name("rpt-dq-fail"))

    # n_files=2, rows_per_file=10 → 20 total rows; inject_errors=10 → 50% error rate
    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        payload={
            "date": "2026-06-02",
            "n_files": 2,
            "rows_per_file": 10,
            "seed": 99,
            "inject_errors": 10,
        },
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] in {"FAILED", "DEAD_LETTER"}, (
        f"Expected FAILED or DEAD_LETTER, got {job['state']}"
    )


# ---------------------------------------------------------------------------
# Step outputs are populated after completion
# ---------------------------------------------------------------------------


async def test_step_outputs_populated_after_completion(client):
    """After a DailyImportJob completes, step-outputs count equals n_files."""
    tenant_id = await create_tenant(client, name=unique_name("rpt-steps"))
    n_files = 5

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        payload={"date": "2026-06-03", "n_files": n_files, "rows_per_file": 50, "seed": 7},
    )

    job = await wait_for_job(client, job_id, timeout=120.0)
    assert job["state"] == "COMPLETED", f"Expected COMPLETED, got {job['state']}"

    step_data = await _get_step_outputs(client, job_id)
    assert step_data["count"] >= n_files, (
        f"Expected at least {n_files} step outputs, got {step_data['count']}"
    )
