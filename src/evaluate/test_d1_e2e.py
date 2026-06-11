"""D1 — End-to-end completion (demonstration).

Submit one DailyImportJob (20 files × 500 rows, seed=42, inject_errors=5 ≈ 1% error
rate — well below the 10% threshold). Assert:
  1. job reaches COMPLETED
  2. .xlsx artifact exists on disk
  3. step_outputs count == n_files (one ingest step per file)
  4. event log contains QUEUED → SCHEDULED → RUNNING → COMPLETED in order
"""

from pathlib import Path

import pytest

from src.evaluate import helpers as h

PAYLOAD = {
    "date": "2024-01-15",
    "n_files": 20,
    "rows_per_file": 500,
    "seed": 42,
    "inject_errors": 5,
}


@pytest.mark.demonstration
def test_d1_e2e_completion(tenant_id: str):
    resp = h.submit_report(tenant_id, PAYLOAD)
    job_id = str(resp["job_id"])

    job = h.wait_for_state(job_id, "COMPLETED", timeout=300)

    # 1. Job reached COMPLETED
    assert job["state"] == "COMPLETED"

    # 2. Artifact exists on disk
    report = h.get_report(job_id)
    artifact_path = report["artifact_path"]
    assert artifact_path, "artifact_path is empty"
    assert Path(artifact_path).exists(), f"Artifact file not found at {artifact_path!r}"
    assert artifact_path.endswith(".xlsx"), f"Artifact extension unexpected: {artifact_path!r}"

    # 3. step_outputs count == n_files (ingest stage, one step per new file)
    step_data = h.get_step_outputs(job_id)
    assert step_data["count"] == PAYLOAD["n_files"], (
        f"Expected {PAYLOAD['n_files']} step_outputs, got {step_data['count']}"
    )
    stage_names = {s["stage_name"] for s in step_data["step_outputs"]}
    assert stage_names == {"ingest"}, f"Unexpected stages in step_outputs: {stage_names}"

    # 4. Event sequence: QUEUED → SCHEDULED → RUNNING → COMPLETED in chronological order
    events = h.get_events(job_id)
    event_types = [e["event"] for e in events]
    for required in ("QUEUED", "SCHEDULED", "RUNNING", "COMPLETED"):
        assert required in event_types, f"Event {required!r} missing from log: {event_types}"

    def idx(name):
        return next(i for i, e in enumerate(events) if e["event"] == name)

    assert idx("QUEUED") < idx("SCHEDULED") < idx("RUNNING") < idx("COMPLETED"), (
        f"Event order wrong: {event_types}"
    )
