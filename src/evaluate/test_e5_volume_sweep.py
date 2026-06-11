"""E5 — Data-volume sweep (RQ2 secondary experiment).

Single worker, single job, rows_per_file swept across [500, 1000, 2000, 4000]
with n_files=5 fixed. 3 repetitions per configuration.

Seed scheme: seed = SEED_BASE + rows_per_file * 10 + rep
Every (rows_per_file, rep) pair gets a unique seed so discover always finds
n_files new files, even with the DB intact across configurations.

Validity assertion: step_outputs count == n_files asserted on every run.
A degenerate zero-file run (seed collision) is counted as a discrepancy.

Results written to: loadtest-results/e5/<timestamp>.csv
One CSV row per (rows_per_file, rep).
"""

from pathlib import Path

import pytest

from src.evaluate import helpers as h

N_FILES = 5
ROWS_SWEEP = [500, 1000, 2000, 4000]
INJECT_ERRORS = 3
SEED_BASE = 50000   # E5 namespace; seed = SEED_BASE + rows_per_file * 10 + rep
N_REPS = 3


@pytest.mark.experiment
def test_e5_volume_sweep(tenant_id: str):
    meta = h.result_metadata()
    csv_rows: list[dict] = []
    discrepancies: list[str] = []

    for rows_per_file in ROWS_SWEEP:
        for rep in range(1, N_REPS + 1):
            seed = SEED_BASE + rows_per_file * 10 + rep
            print(
                f"\n[E5] rows_per_file={rows_per_file}, rep {rep}/{N_REPS}, seed={seed}"
            )

            payload = {
                "n_files": N_FILES,
                "rows_per_file": rows_per_file,
                "seed": seed,
                "inject_errors": INJECT_ERRORS,
                "date": f"2024-09-{rows_per_file // 100:02d}",
            }

            resp = h.submit_report(tenant_id, payload, timeout_seconds=900)
            job_id = str(resp["job_id"])

            row: dict = {
                "git_commit": meta["git_commit"],
                "timestamp": meta["timestamp"],
                "n_files": N_FILES,
                "rows_per_file": rows_per_file,
                "total_records": N_FILES * rows_per_file,
                "seed": seed,
                "rep": rep,
                "job_id": job_id,
                **{f"setting_{k}": v for k, v in meta["settings"].items()},
            }

            try:
                job = h.wait_for_state(job_id, "COMPLETED", timeout=900)
            except (TimeoutError, AssertionError) as exc:
                discrepancies.append(f"rows={rows_per_file} rep={rep}: {exc}")
                row.update({"state": "TIMEOUT_OR_ERROR", "duration_s": None,
                            "step_outputs_count": None})
                csv_rows.append(row)
                continue

            started = job.get("started_at")
            completed = job.get("completed_at")
            duration_s = (
                round(h.epoch(completed) - h.epoch(started), 3)
                if started and completed
                else None
            )

            # Validity: must have processed n_files
            step_count = h.get_step_outputs(job_id)["count"]
            if step_count != N_FILES:
                discrepancies.append(
                    f"rows={rows_per_file} rep={rep}: "
                    f"step_outputs={step_count}, expected {N_FILES}; "
                    f"possible seed collision — run is invalid"
                )

            print(
                f"[E5] rows_per_file={rows_per_file} rep={rep}: "
                f"duration={duration_s}s ({N_FILES * rows_per_file} records)"
            )

            row.update({
                "state": job["state"],
                "duration_s": duration_s,
                "step_outputs_count": step_count,
            })
            csv_rows.append(row)

    out = Path(h.RESULTS_DIR) / "e5" / f"{h.ts_now()}.csv"
    h.write_csv(out, csv_rows)

    if discrepancies:
        pytest.fail(
            f"E5: {len(discrepancies)} run(s) failed validity:\n"
            + "\n".join(f"  • {d}" for d in discrepancies)
        )
