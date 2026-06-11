"""D2 — Quality gate (demonstration).

Submit with inject_errors=1000 across n_files=10, rows_per_file=500
(total 5000 records; ~18% expected error rate — above the 10% threshold).
Assert:
  1. every attempt fails with the threshold ValueError (error_message contains "exceeds threshold")
  2. job reaches DEAD_LETTER after exactly max_retries attempts
  3. a dead_letter_jobs row exists with final_error containing the error rate
  4. events show the full attempt/RETRIED sequence (RUNNING × max_retries, RETRIED × (max_retries-1))
"""

import pytest

from src.evaluate import helpers as h

# inject_errors=1000 in 10×500=5000 positions → ~18% error rate (above 10% threshold)
PAYLOAD = {
    "date": "2024-02-01",
    "n_files": 10,
    "rows_per_file": 500,
    "seed": 99,
    "inject_errors": 1000,
}
MAX_RETRIES = 3


@pytest.mark.demonstration
def test_d2_quality_gate(tenant_id: str):
    resp = h.submit_report(tenant_id, PAYLOAD, max_retries=MAX_RETRIES, timeout_seconds=300)
    job_id = str(resp["job_id"])

    # Wait; retry delays are short (RETRY_BASE_DELAY=2s) but 3 attempts take ~30s total
    job = h.wait_for_state(job_id, "DEAD_LETTER", timeout=300)

    # 1. Terminal state is DEAD_LETTER
    assert job["state"] == "DEAD_LETTER"

    # 2. error_message on job row contains the threshold phrase
    assert job["error_message"], "error_message is empty"
    assert "exceeds threshold" in job["error_message"], (
        f"error_message does not mention threshold: {job['error_message']!r}"
    )

    # 3. dead_letter_jobs row exists with matching final_error
    dl_list = h.db_fetchall(
        "SELECT dlj.final_error, dlj.total_attempts "
        "FROM dead_letter_jobs dlj WHERE dlj.job_id=%s",
        (job_id,),
    )
    assert len(dl_list) == 1, f"Expected 1 dead_letter_jobs row, got {len(dl_list)}"
    final_error, total_attempts = dl_list[0]
    assert "exceeds threshold" in (final_error or ""), (
        f"dead_letter_jobs.final_error: {final_error!r}"
    )

    # 4. total_attempts == MAX_RETRIES (attempt increments to MAX_RETRIES before dead-lettering)
    assert total_attempts == MAX_RETRIES, (
        f"Expected total_attempts={MAX_RETRIES}, got {total_attempts}"
    )

    # 5. RUNNING events == MAX_RETRIES, RETRIED events == MAX_RETRIES - 1
    events = h.get_events(job_id)
    running_count = sum(1 for e in events if e["event"] == "RUNNING")
    retried_count = sum(1 for e in events if e["event"] == "RETRIED")

    assert running_count == MAX_RETRIES, (
        f"Expected {MAX_RETRIES} RUNNING events, got {running_count}"
    )
    assert retried_count == MAX_RETRIES - 1, (
        f"Expected {MAX_RETRIES - 1} RETRIED events, got {retried_count}"
    )
