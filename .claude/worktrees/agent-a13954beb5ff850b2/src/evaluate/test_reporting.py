"""
Evaluation tests for DailyImportJob via the reports API.
Requires running docker stack: docker-compose up -d --build
"""
import pytest
from src.evaluate.helpers import (
    create_tenant,
    wait_for_job,
    unique_name,
    TERMINAL_STATES,
)

pytestmark = [pytest.mark.evaluate, pytest.mark.e2e]


@pytest.mark.asyncio
async def test_daily_import_end_to_end(client, api_url, require_stack):
    """DailyImportJob completes with .xlsx artifact and quality summary."""
    tenant_id = await create_tenant(client, name=unique_name("tenant"))

    # Submit via reports API
    resp = await client.post(f"{api_url}/api/v1/reports", json={
        "tenant_id": tenant_id,
        "report_type": "daily_import",
        "payload": {"tenant_id": tenant_id, "n_files": 5, "rows_per_file": 100, "seed": 1, "inject_errors": 2},
    })
    assert resp.status_code in (200, 201), resp.text
    job_id = resp.json()["job_id"]

    job = await wait_for_job(client, job_id, timeout=120)
    assert job["state"] == "COMPLETED", (
        f"Expected COMPLETED, got {job['state']}: {job.get('error_message')}"
    )

    # Verify artifact
    report_resp = await client.get(f"{api_url}/api/v1/reports/{job_id}")
    assert report_resp.status_code == 200
    report = report_resp.json()
    artifact_path = report.get("artifact_path", "")
    assert artifact_path.endswith(".xlsx"), f"Expected .xlsx artifact, got: {artifact_path}"

    # Verify summary in result
    result = job.get("result", {})
    assert result is not None
    summary = result.get("summary", {})
    assert "total_records" in summary or "valid_count" in summary, (
        f"Missing summary in result: {result}"
    )


@pytest.mark.asyncio
async def test_daily_import_quality_failure(client, api_url, require_stack):
    """DailyImportJob fails when inject_errors exceeds quality threshold."""
    tenant_id = await create_tenant(client, name=unique_name("tenant"))

    # Inject way too many errors (50%+ of all records for 5 files * 100 rows = 500 records)
    resp = await client.post(f"{api_url}/api/v1/reports", json={
        "tenant_id": tenant_id,
        "report_type": "daily_import",
        "payload": {"tenant_id": tenant_id, "n_files": 5, "rows_per_file": 100, "seed": 2, "inject_errors": 300},
    })
    assert resp.status_code in (200, 201), resp.text
    job_id = resp.json()["job_id"]

    job = await wait_for_job(client, job_id, target_states=TERMINAL_STATES, timeout=120)
    assert job["state"] in ("FAILED", "DEAD_LETTER"), (
        f"Expected failure, got: {job['state']}"
    )


@pytest.mark.asyncio
async def test_daily_import_step_outputs_populated(client, api_url, require_stack):
    """After DailyImportJob completes, step-outputs endpoint returns one entry per file."""
    tenant_id = await create_tenant(client, name=unique_name("tenant"))
    n_files = 5

    resp = await client.post(f"{api_url}/api/v1/reports", json={
        "tenant_id": tenant_id,
        "report_type": "daily_import",
        "payload": {"tenant_id": tenant_id, "n_files": n_files, "rows_per_file": 50, "seed": 3, "inject_errors": 0},
    })
    assert resp.status_code in (200, 201)
    job_id = resp.json()["job_id"]

    job = await wait_for_job(client, job_id, timeout=120)
    assert job["state"] == "COMPLETED"

    # Check step outputs — one per file ingested
    step_resp = await client.get(f"{api_url}/api/v1/jobs/{job_id}/step-outputs")
    assert step_resp.status_code == 200
    data = step_resp.json()
    assert data["count"] >= n_files, (
        f"Expected >= {n_files} step outputs, got {data['count']}"
    )
