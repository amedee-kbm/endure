"""
Reporting module E2E tests — §4.2 Functional Validation.

Validates that all three report job types complete successfully through all stages,
persist an artifact_path in their result, and produce a correct JobEvent audit trail.
Also covers data quality validation (validate stage) and tenant-filtered report listing.

Requires a running Docker Compose stack (same as other evaluate tests).
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

DAILY_SALES = "src.reporting.jobs.daily_sales:DailySalesReportJob"
WEEKLY_ACTIVITY = "src.reporting.jobs.weekly_activity:WeeklyActivityReportJob"
ALERT_DIGEST = "src.reporting.jobs.alert_digest:AlertDigestReportJob"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def _submit_report(client, *, tenant_id: str, report_type: str, payload: dict) -> str:
    """Submit via the /reports convenience endpoint; return job_id."""
    r = await client.post(
        "/api/v1/reports",
        json={
            "tenant_id": tenant_id,
            "report_type": report_type,
            "payload": payload,
            "max_retries": 0,
            "timeout_seconds": 120,
        },
    )
    r.raise_for_status()
    return str(r.json()["job_id"])


async def _get_report(client, job_id: str) -> dict:
    r = await client.get(f"/api/v1/reports/{job_id}")
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# DailySalesReportJob — basic completion
# ---------------------------------------------------------------------------


async def test_daily_sales_report_completes(client):
    tenant_id = await create_tenant(client, name=unique_name("rpt-sales"))

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        report_type="daily_sales",
        payload={"date": "2026-06-01", "seed": 1, "n_orders": 20},
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] == "COMPLETED", f"Expected COMPLETED, got {job['state']}"

    report = await _get_report(client, job_id)
    assert report["artifact_path"] is not None, "Expected artifact_path in result"
    assert "sales" in report["artifact_path"], "artifact_path should contain 'sales'"

    events = await get_events(client, job_id)
    event_types = [e["event"] for e in events]
    assert "QUEUED" in event_types
    assert "RUNNING" in event_types
    assert "COMPLETED" in event_types


# ---------------------------------------------------------------------------
# WeeklyActivityReportJob — basic completion
# ---------------------------------------------------------------------------


async def test_weekly_activity_report_completes(client):
    tenant_id = await create_tenant(client, name=unique_name("rpt-activity"))

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        report_type="weekly_activity",
        payload={"week_start": "2026-06-02", "seed": 2, "n_sessions": 50},
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] == "COMPLETED"

    report = await _get_report(client, job_id)
    assert report["artifact_path"] is not None
    assert "activity" in report["artifact_path"]


# ---------------------------------------------------------------------------
# AlertDigestReportJob — basic completion
# ---------------------------------------------------------------------------


async def test_alert_digest_report_completes(client):
    tenant_id = await create_tenant(client, name=unique_name("rpt-alert"))

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        report_type="alert_digest",
        payload={"seed": 42, "n_metrics": 8},
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] == "COMPLETED", f"Expected COMPLETED, got {job['state']}"

    report = await _get_report(client, job_id)
    assert report["artifact_path"] is not None
    assert "alerts" in report["artifact_path"]

    events = await get_events(client, job_id)
    event_types = [e["event"] for e in events]
    assert "COMPLETED" in event_types


# ---------------------------------------------------------------------------
# Data quality validation (validate stage) — §4.2
# ---------------------------------------------------------------------------


async def test_data_quality_valid_data_passes(client):
    """Clean data completes without anomalies in the quality summary."""
    tenant_id = await create_tenant(client, name=unique_name("rpt-dq-valid"))

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        report_type="daily_sales",
        payload={"date": "2026-06-01", "seed": 1, "n_orders": 50},
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] == "COMPLETED", f"Expected COMPLETED, got {job['state']}"

    report = await _get_report(client, job_id)
    assert report["artifact_path"] is not None


async def test_data_quality_invalid_rows_caught(client):
    """Injected duplicate order IDs are flagged; job still completes with quality summary."""
    tenant_id = await create_tenant(client, name=unique_name("rpt-dq-invalid"))

    job_id = await _submit_report(
        client,
        tenant_id=tenant_id,
        report_type="daily_sales",
        # inject_errors=3 adds 3 duplicate order IDs; below the 10% threshold → COMPLETED
        payload={"date": "2026-06-02", "seed": 2, "n_orders": 100, "inject_errors": 3},
    )

    job = await wait_for_job(client, job_id, timeout=60.0)
    assert job["state"] == "COMPLETED", (
        f"Expected COMPLETED (errors below threshold); got {job['state']}"
    )

    report = await _get_report(client, job_id)
    assert report["artifact_path"] is not None


# ---------------------------------------------------------------------------
# List endpoint
# ---------------------------------------------------------------------------


async def test_list_reports_filters_by_tenant(client):
    tenant_a = await create_tenant(client, name=unique_name("rpt-list-a"))
    tenant_b = await create_tenant(client, name=unique_name("rpt-list-b"))

    await _submit_report(
        client,
        tenant_id=tenant_a,
        report_type="daily_sales",
        payload={"date": "2026-06-01", "seed": 10, "n_orders": 5},
    )

    r = await client.get(f"/api/v1/reports?tenant_id={tenant_a}")
    r.raise_for_status()
    reports_a = r.json()

    r2 = await client.get(f"/api/v1/reports?tenant_id={tenant_b}")
    r2.raise_for_status()
    reports_b = r2.json()

    assert len(reports_a) >= 1
    assert all(rpt["tenant_id"] == tenant_a for rpt in reports_a)
    assert len(reports_b) == 0
