"""P1/P2 pipeline obligations, in-process against the real DailyImportJob."""

from __future__ import annotations

import pytest

from src.models import SourceFile
from src.worker.executor import JobExecutor

pytestmark = pytest.mark.asyncio

DAILY = "src.reporting.jobs.daily_import:DailyImportJob"


def _payload(tenant, seed: int, inject_errors: int) -> dict:
    return {
        "tenant_id": str(tenant.id),
        "date": "2026-06-01",
        "n_files": 10,
        "rows_per_file": 200,
        "seed": seed,
        "inject_errors": inject_errors,
    }


async def test_quality_gate_converts_silent_to_loud(make_job, tenant):
    """P2: error rate above threshold raises — silent corruption becomes a
    loud, scheduler-visible failure."""
    job = await make_job(job_type=DAILY)
    # 10 × 200 = 2000 rows; ≥ 400 injected errors guarantees > 10% rate.
    result = await JobExecutor().execute(
        DAILY, _payload(tenant, seed=7001, inject_errors=400), job_id=job.id
    )
    assert result["success"] is False
    assert "error rate" in result["error"].lower()


async def test_cross_job_idempotency_by_content_hash(make_job, tenant):
    """P1: a second run over the same files registers and ingests nothing."""
    payload = _payload(tenant, seed=7002, inject_errors=0)

    job1 = await make_job(job_type=DAILY)
    r1 = await JobExecutor().execute(DAILY, payload, job_id=job1.id)
    assert r1["success"] is True
    registered = await SourceFile.objects.filter(tenant=tenant).acount()
    assert registered == 10

    job2 = await make_job(job_type=DAILY)
    r2 = await JobExecutor().execute(DAILY, payload, job_id=job2.id)
    assert r2["success"] is True
    assert r2["result"]["file_count"] == 0, "re-discovered already-hashed files"
    assert await SourceFile.objects.filter(tenant=tenant).acount() == registered
