"""D3 — Cross-job idempotency (demonstration).

Run the same payload (same seed) twice to completion. Assert:
  1. both jobs reach COMPLETED
  2. second job's discover stage registers zero new files
     (step_outputs count == 0 — no ingest steps needed)
  3. source_files row count is unchanged after the second run
"""

import pytest

from src.evaluate import helpers as h

# Distinct seed from D1 to avoid cross-test SourceFile interference
PAYLOAD = {
    "date": "2024-03-01",
    "n_files": 10,
    "rows_per_file": 200,
    "seed": 77,
    "inject_errors": 2,
}


@pytest.mark.demonstration
def test_d3_cross_job_idempotency(tenant_id: str):
    # --- Run 1 ---
    resp1 = h.submit_report(tenant_id, PAYLOAD)
    job1_id = str(resp1["job_id"])
    h.wait_for_state(job1_id, "COMPLETED", timeout=300)
    assert h.get_job(job1_id)["state"] == "COMPLETED"

    # Record source_files count after first run (archive creates SourceFile records)
    sf_after_run1 = h.source_file_count(tenant_id)
    assert sf_after_run1 > 0, "No SourceFile records created after first run"

    # --- Run 2 (same payload) ---
    resp2 = h.submit_report(tenant_id, PAYLOAD)
    job2_id = str(resp2["job_id"])
    h.wait_for_state(job2_id, "COMPLETED", timeout=300)
    assert h.get_job(job2_id)["state"] == "COMPLETED"

    # 1. Both completed (asserted above)

    # 2. Second job processed zero new files — all already in source_files
    step_data = h.get_step_outputs(job2_id)
    assert step_data["count"] == 0, (
        f"Second run should have 0 step_outputs (all files already ingested), "
        f"got {step_data['count']}"
    )

    # Verify via job result: file_count should be 0
    job2 = h.get_job(job2_id)
    result = job2.get("result") or {}
    file_count = result.get("file_count")
    if file_count is not None:  # present in result dict
        assert file_count == 0, (
            f"Second run result.file_count={file_count!r}, expected 0"
        )

    # 3. source_files count unchanged after second run
    sf_after_run2 = h.source_file_count(tenant_id)
    assert sf_after_run2 == sf_after_run1, (
        f"source_files count changed: {sf_after_run1} → {sf_after_run2}; "
        f"second run must not insert new rows"
    )
