# INTEGRATION — final three-tier evaluation suite

Copy the `src/` tree from this package into the repo, apply `PATCHES.md`
FIRST (a Tier 1 test fails without it, by design), then wire the items below.
Report back per the protocol at the bottom.

## 0. Dependencies and pytest config

Add to the dev dependency group if absent: `pytest`, `pytest-asyncio`.
Add to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = [
  "live: requires the full docker compose stack",
  "demonstration: legacy marker, may be removed with the old suite",
]
```

## 1. What replaces what

- DELETE: `test_d1_e2e.py`, `test_d2_quality_gate.py`, `test_d3_idempotency.py`,
  `test_d4_arbitration.py`, `test_e1_stage_recovery.py`,
  `test_e2_step_recovery.py`, `test_e3_coordinator_failover.py`.
  Their coverage now lives in `tier1/` (deterministic, seconds) and `tier2/`
  (two single live demos).
- KEEP: `helpers.py`, `conftest.py` (session tenant — verify no marker
  collisions), `test_e4_worker_sweep.py` (now "E4b"), `run_e4_sweep.sh`,
  `test_e5_volume_sweep.py`.
- ADD: everything under `tier1/`, `tier2/`, `tier3/`, plus
  `src/reporting/jobs/sleep_job.py`.

## 2. Verification points (things written against my reading of the repo —
confirm each, adjust if reality differs, and REPORT any adjustment)

a. `Scheduler` class name and that `_detect_dead_workers`,
   `_detect_timed_out_jobs`, `_handle_job_failure` are callable on a bare
   `Scheduler()` instance without `start()` (tier1/test_time_injection.py).
b. `Tenant` creation via `aget_or_create(name=...)` — if the model has other
   required fields, extend the tier1 conftest fixture.
c. The tier1 conftest DB env-var names match `endure/settings.py`
   (`ENDURE_DATABASE_{NAME,USER,PASSWORD,HOST,PORT}`).
d. `helpers._api("POST", "/jobs", json=...)` — confirm the jobs router path
   and `JobCreate` field names used in `tier3/test_sleep_sweep.py`.
e. `helpers.capture_settings()` keys used in tier2:
   `worker_heartbeat_timeout`, `worker_heartbeat_interval`,
   `scheduler_loop_interval`, `leader_lock_ttl`, `leader_heartbeat_interval`.
   Rename in the tests if helpers uses different keys.
f. `helpers.get_step_outputs(job_id)` return shape (count vs items list) —
   tier2 handles both, but confirm.
g. `wait_for_worker_offline`, `wait_for_leader_change`, `holder_to_container`,
   `kill_one`, `kill_named` signatures as used in tier2.

## 3. E4b and E5 adjustments (existing files)

- Reduce reps from 3 to 2 in `test_e4_worker_sweep.py` and
  `test_e5_volume_sweep.py`.
- Import `DrainSampler` from `src.evaluate.tier3.test_sleep_sweep` (or move it
  into `helpers.py` — preferred) and wrap E4b's measurement loop with it,
  writing `drain_wN_<ts>.csv` beside the existing CSVs.
- Ensure E4b rows include a `makespan_s` column (analyze.py keys on it).

## 4. Run order (for the human, after integration)

```
# Tier 1 — seconds; only postgres needed
docker compose up -d postgres
pytest src/evaluate/tier1/ -v

# Tier 2 — full stack; two single demos, ~5 min
dc up -d --scale worker=1 --wait
dc run --rm runner pytest src/evaluate/tier2/test_live_worker_kill.py -v
dc run --rm runner pytest src/evaluate/tier2/test_live_leader_kill.py -v

# Tier 3 — throughput
#   E4a sleep sweep: per N in 1 2 4 8:
#     dc up -d --scale worker=N --wait
#     dc run --rm -e ENDURE_E4_WORKERS=N runner pytest src/evaluate/tier3/test_sleep_sweep.py -v
#   E4b real-job sweep: bash src/evaluate/run_e4_sweep.sh    (now 2 reps)
#   E5: as before
python src/evaluate/tier3/analyze.py loadtest-results/e4a
python src/evaluate/tier3/analyze.py loadtest-results/e4
```

## 5. Report-back protocol

1. Every verification point in §2: confirmed as written, or what was changed.
2. Tier 1: full pytest output. `test_double_crash_resume` must FAIL before
   PATCHES.md and PASS after — run it both ways and show both outputs.
3. Do not weaken any assertion to make a test pass. If an assertion fails,
   report it verbatim with the surrounding context.
4. List any file where integration required edits beyond §2's points.
