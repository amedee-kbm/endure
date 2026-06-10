# Endure Evaluation Tests

This package contains the evaluation chapter tests for the thesis. They cover three research
questions:

| RQ | Question |
|----|----------|
| **RQ1** | Does throughput scale linearly with the number of worker nodes? |
| **RQ2** | Does the system recover from faults (worker crashes, scheduler failover) without losing jobs? |

All tests run against a **live Docker Compose stack**. They are not unit tests.

---

## Directory structure

```
evaluate/
├── README.md                    ← this file
├── conftest.py                  ← session fixtures (api_url, client, require_stack)
├── helpers.py                   ← shared HTTP utilities and assertions
├── jobs.py                      ← synthetic job implementations (SyntheticJob)
│
├── test_job_lifecycle.py        ← RQ1/RQ2 smoke: submit → COMPLETED + audit trail
├── test_dlq.py                  ← RQ2: failed jobs exhaust retries and land in DLQ
├── test_chaos_recovery.py       ← RQ2: worker crash and scheduler failover (chaos)
├── test_checkpoint.py           ← RQ2: checkpoint resume after worker kill (chaos)
├── test_leader_metrics.py       ← operational: leader election and metrics endpoints
├── test_reporting.py            ← functional validation: report job types, artifacts, audit trail
├── test_process_isolation.py    ← endure-specific: subprocess isolation
├── test_periodic_tasks.py       ← endure-specific: cron-based periodic tasks
│
└── load/                        ← RQ1 load testing (see below)
    ├── k6/
    │   └── script.js            ← k6 scenario (primary load tool)
    ├── locustfile.py            ← Locust scenario (secondary)
    ├── run_volume_sweep.py      ← Table 4.1: DailySalesReportJob completion time vs. n_orders
    ├── run_k6_sweep.py          ← Table 4.2: SyntheticJob throughput vs. worker count (k6)
    └── run_worker_sweep.py      ← Table 4.2 alternative: Locust-based worker sweep
```

Results land in `loadtest-results/` at the project root.

---

## Prerequisites

```bash
# Build the Docker image (once)
docker compose build

# Install Python dev dependencies (includes httpx, locust, pytest-asyncio)
uv sync
```

For the load tests only:

```bash
# Install k6 (Windows)
winget install k6
# or: https://grafana.com/docs/k6/latest/set-up/install-k6/
```

---

## Running the tests

### pytest-based tests (RQ1 smoke, RQ2)

```bash
# Non-chaos tests — 1 worker is sufficient
docker compose up -d
ENDURE_API_URL=http://localhost:8000 uv run pytest src/evaluate/ -m "e2e and not chaos" -v
docker compose down -v

# Chaos tests — need 2 workers so one can be killed and jobs re-assigned to the other
docker compose up -d --scale worker=2
ENDURE_CHAOS=1 uv run pytest src/evaluate/ -m "chaos" -v
docker compose down -v
```

**Run a single file:**

```bash
uv run pytest src/evaluate/test_job_lifecycle.py -v
```

### Load tests — thesis results

Two scripts produce the tables in the thesis chapter. Both manage Docker themselves.

**Table 4.1 — volume sweep** (`run_volume_sweep.py`):
Report job completion time vs. data volume. Submits `DailySalesReportJob` at each
`n_orders` level (200 → 100 000) and records wall-clock completion time.

```bash
MANAGE_STACK=1 uv run python src/evaluate/load/run_volume_sweep.py
```

Results written to `loadtest-results/volume-sweep/volume_summary.json`.

**Table 4.2 — worker scalability sweep** (`run_k6_sweep.py`):
SyntheticJob throughput vs. worker count. Adds workers one at a time and fires a
constant-arrival-rate burst at each count.

```bash
# Reproduce the thesis result
FIXED_RATE=8 uv run python src/evaluate/load/run_k6_sweep.py

# Re-run a single worker count (e.g. w=4 only)
MIN_WORKERS=4 MAX_WORKERS=4 FIXED_RATE=8 \
    uv run python src/evaluate/load/run_k6_sweep.py
```

Results written to `loadtest-results/worker-sweep-k6/summary.json`.

---

## Markers

| Marker | Meaning | Enabled by |
|--------|---------|-----------|
| `e2e` | Requires running stack | Default when stack is up |
| `chaos` | Kills containers | `ENDURE_CHAOS=1` |
| `evaluate` | All evaluation chapter tests | Always |

The `require_stack` fixture in `conftest.py` skips the entire session if the API is
unreachable, so tests fail gracefully when the stack is not running.

---

## Synthetic jobs (`jobs.py`)

Two job types are used across all tests. They must be importable from inside the Docker
container (`PYTHONPATH=/app` covers the path).

### `SyntheticJob` — `src.evaluate.jobs:SyntheticJob`

A multi-stage job with configurable duration, workload character, and optional failure
injection.

| Payload field | Type | Default | Description |
|---------------|------|---------|-------------|
| `stage_duration` | float | `0.3` | Seconds per stage |
| `stages` | int | `5` | Number of stages to run |
| `fail_at_stage` | int | `None` | 0-based stage index to raise on |

Supports checkpointing: saves completed stage names so a resumed job skips already-done
stages. Total execution time ≈ `stage_duration × stages`.

---

## Shared utilities (`helpers.py`)

| Function | Description |
|----------|-------------|
| `create_tenant(client, *, name, max_concurrent_jobs, max_workers)` | Creates a tenant; handles 409 by lookup |
| `submit_job(client, *, tenant_id, job_type, payload, ...)` | Submits a job, returns its UUID |
| `wait_for_job(client, job_id, *, target_states, timeout)` | Polls until the job reaches one of the target states |
| `wait_for_jobs(client, job_ids, *, timeout)` | Waits for all listed jobs to reach terminal states |
| `wait_for_running_count(client, job_ids, min_running, *, timeout)` | Waits until at least N of the listed jobs are RUNNING |
| `wait_for_checkpoint(client, job_id, *, min_total, timeout)` | Waits until the job has saved at least N checkpoints |
| `get_events(client, job_id)` | Returns the full event log for a job |
| `get_assigned_worker_hostname(client, job_id)` | Resolves the Docker container hostname of the assigned worker |

---

## Test descriptions

### `test_job_lifecycle.py` — RQ1/RQ2 smoke

Submits one `SyntheticJob` and verifies it reaches COMPLETED with the correct event sequence
(QUEUED → RUNNING → COMPLETED). Acts as a sanity check before running heavier tests.

### `test_dlq.py` — RQ2

Submits a `SyntheticJob` configured to fail at stage 0 with `max_retries=2`. Verifies the
job reaches DEAD_LETTER after exhausting all retry attempts, and that it appears in
`GET /api/v1/admin/dead-letter` with `total_attempts=2`.

### `test_chaos_recovery.py` — RQ2 (chaos)

Two tests, both require `ENDURE_CHAOS=1`:

**`test_worker_crash_jobs_still_complete`**
Submits 5 jobs, waits until ≥3 are RUNNING, then kills the `endure-worker-1` container.
Asserts all 5 jobs eventually COMPLETE (scheduler detects dead worker via heartbeat timeout
and re-queues orphaned jobs). Worker container is restarted in the `finally` block.

**`test_scheduler_failover_jobs_complete`**
Submits 8 staggered jobs, kills the primary scheduler container once some jobs are RUNNING.
Asserts the standby scheduler acquires leadership within 60 seconds and all 8 jobs
eventually COMPLETE. Scheduler container is restarted in the `finally` block.

Override container names with env vars:
- `ENDURE_WORKER_CONTAINER` (default: `endure-worker-1`)
- `ENDURE_SCHEDULER_CONTAINER` (default: `endure-scheduler`)

### `test_checkpoint.py` — RQ2 (chaos)

Requires `ENDURE_CHAOS=1`. Submits a `SyntheticJob` with `stage_duration=2.0, stages=5,
max_retries=3`. Waits for 2 checkpoints to be saved, then kills the assigned worker.
Verifies the job eventually COMPLETES and that an event detail contains `"skip"` (proof
that the resumed execution skipped already-completed stages rather than re-running from
scratch).

Checkpoints are saved at stage boundaries; the test waits for two stage completions before killing the worker.

### `test_leader_metrics.py` — operational

Two lightweight smoke tests that don't require any job submission:
- `GET /api/v1/admin/leader` → 200, body contains a `leader` object with `holder_id`
- `GET /api/v1/metrics` → 200, body contains `jobs`, `queue`, and `workers` keys

### `test_process_isolation.py` — endure-specific

Tests the subprocess isolation feature (not present in the original scheduler prototype).
Verifies jobs complete correctly under process isolation and that failures are recorded
properly.

### `test_periodic_tasks.py` — endure-specific

Tests cron-based `PeriodicTask` scheduling. Creates a periodic task and waits for the
scheduler to spawn and complete a job automatically.

---

## Load tests — RQ1 (`load/`)

### Methodology

The load tests answer RQ1 quantitatively by measuring actual job throughput and end-to-end
latency across worker counts 1–8.

**Why k6 instead of Locust?**
Locust uses a *closed model*: each virtual user blocks polling until its job completes, then
submits the next. Throughput depends on user count and job duration, requiring careful
calibration per configuration. k6's `constant-arrival-rate` executor uses an *open model*:
it fires N new iterations per second regardless of how long each takes, auto-allocating VUs.
The system's actual completion rate is the measurement — no user-count tuning needed.

**Fixed-rate overload approach**
Set `FIXED_RATE` above the maximum capacity of the system. k6 tries to submit at that rate;
the actual completion count divided by the measurement window gives the true throughput
ceiling. At low worker counts the system is overloaded; at higher counts it handles the full
load. This reveals:

- **Overloaded regime:** `jobs_per_s < FIXED_RATE` — actual throughput ceiling; latency is
  dominated by queue wait time.
- **Handled regime:** `jobs_per_s ≈ FIXED_RATE` — system has headroom; latency collapses to
  the execution floor.
- **Knee point:** the minimum worker count at which the system transitions from overloaded to
  handled.

**Latency interpretation**

`p50_ms` is the wall-clock time a user waits from job submission to terminal state:

```
p50 = queue_wait_time + job_execution_time + scheduling_overhead
```

In the overloaded regime, queue wait dominates (10–30× execution time). In the handled
regime, p50 reflects the execution floor (`stage_duration × stages + ~0.6s` overhead).

### Scripts

**`run_volume_sweep.py`** — Table 4.1: DailySalesReportJob completion time vs. data volume

Submits `DailySalesReportJob` at each `n_orders` level (200 → 100,000), waits for each to complete, and records wall-clock completion time. Requires a stack with the correct worker count already running (or set `MANAGE_STACK=1` to have the script manage it).

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKERS` | `4` | Fixed worker count |
| `RUNS` | `5` | Runs per volume level for median |
| `N_ORDERS` | `200,1000,5000,20000,100000` | Comma-separated volume levels |
| `API_URL` | `http://localhost:8000` | Endure API base URL |
| `RESULTS_DIR` | `loadtest-results/volume-sweep` | Output directory |
| `MANAGE_STACK` | `0` | Set to `1` to start/stop Docker Compose automatically |

```bash
# Stack already running with 4 workers:
uv run python src/evaluate/load/run_volume_sweep.py

# Script manages Docker itself:
MANAGE_STACK=1 uv run python src/evaluate/load/run_volume_sweep.py
```

Results are written to `volume_summary.json`.

**`run_k6_sweep.py`** — Table 4.2: SyntheticJob throughput vs. worker count

Starts the full Docker stack once, then adds workers one at a time without restarting the
base services. After each k6 run in fixed-rate mode it flushes the Redis job queue
(`endure:queue:jobs`) and truncates the jobs table to prevent leftover overflow jobs from
contaminating the next count's measurement.

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_WORKERS` | `8` | Highest worker count |
| `MIN_WORKERS` | `1` | Starting worker count |
| `FIXED_RATE` | *(unset)* | Fixed arrival rate for all counts, jobs/s |
| `RATE_PER_WORKER` | `1.3` | Proportional rate per worker when `FIXED_RATE` unset |
| `WARMUP_SECS` | `5` | Seconds excluded from k6 stats at scenario start |
| `DURATION_SECS` | `30` | Measurement window per worker count |
| `STAGE_DURATION` | `0.4` | Seconds per synthetic job stage |
| `ENDURE_USE_PROCESS_ISOLATION` | `"false"` | Set to `"true"` to run workers with subprocess isolation |
| `RESULTS_DIR` | `loadtest-results/worker-sweep-k6` | Output directory |

When `ENDURE_USE_PROCESS_ISOLATION` is set, the sweep rebuilds the Docker image before
starting the stack so the setting takes effect inside the containers.

**`run_worker_sweep.py`** — secondary Locust-based sweep

Proportional-rate mode only. Starts and tears down the full stack for each worker count.
Users scale with workers (`USERS_PER_WORKER × workers`). Use for quick sanity checks or when
k6 is unavailable. Reports `job_completed_latency` from the Locust custom event as the
primary metric.

**`k6/script.js`** — k6 scenario

`setup()` creates (or reuses) a per-worker-count tenant. Each iteration submits one
`SyntheticJob` then polls until terminal. An iteration-level timeout (`DURATION` seconds)
prevents stalled pollers from accumulating when the system is heavily overloaded.

### Recorded thesis results

**Configuration:** `FIXED_RATE=8`, `DURATION_SECS=30`, `STAGE_DURATION=0.4` (2.0s jobs),
`SCHEDULER_LOOP_INTERVAL=0.1s`, 4 slots per worker.

Results stored in `loadtest-results/worker-sweep-k6-final/`.

| Workers | Slots | Actual (jobs/s) | p50 | p95 | Regime |
|---------|-------|----------------|-----|-----|--------|
| 1 | 4 | 1.87 | 14.1s | 27.4s | overloaded |
| 2 | 8 | 4.03 | 11.1s | 18.9s | overloaded |
| 3 | 12 | 6.00 | 7.7s | 12.3s | overloaded |
| 4 | 16 | 7.87 | 4.2s | 5.8s | at knee |
| **5** | **20** | **8.03** | **2.6s** | **3.2s** | **handled** |
| 6 | 24 | 8.03 | 2.6s | 3.1s | handled |
| 7 | 28 | 8.00 | 2.6s | 3.1s | handled |
| 8 | 32 | 8.03 | 2.6s | 3.2s | handled |

**Key findings:**

- Throughput scales linearly in the overloaded regime: each added worker contributes ~2 jobs/s
  (4 slots ÷ 2s per job). From w=1→4: **4.21× gain for 4× workers** (105% linear efficiency).
- The **knee point is w=5**: 5 workers (20 slots, 10 jobs/s capacity) absorb the 8 jobs/s
  load without queueing. Latency drops from 4.2s at w=4 to 2.6s at w=5.
- The **execution floor is 2.6s** (2.0s execution + ~0.6s scheduling/polling overhead), flat
  and stable from w=5 through w=8 — the system adds no per-job coordination overhead as
  scale increases.
- Beyond the knee, adding workers provides no benefit at this load level. A higher
  `FIXED_RATE` shifts the knee right.

### Re-running the sweep

```bash
# Reproduce the baseline
FIXED_RATE=8 RESULTS_DIR=loadtest-results/worker-sweep-k6-final \
    uv run python src/evaluate/load/run_k6_sweep.py

# With process isolation
FIXED_RATE=8 ENDURE_USE_PROCESS_ISOLATION=true \
    RESULTS_DIR=loadtest-results/sweep-isolation \
    uv run python src/evaluate/load/run_k6_sweep.py

# Higher load — shifts knee to ~w=7
FIXED_RATE=12 uv run python src/evaluate/load/run_k6_sweep.py

# Proportional-rate stability check
uv run python src/evaluate/load/run_k6_sweep.py
```

---

## Process isolation experiment — RQ1 extension

To understand when process isolation matters for throughput, the sweep was recorded three
times under the same fixed load (`FIXED_RATE=8`, `DURATION_SECS=30`, `STAGE_DURATION=0.4`):

1. **I/O baseline** (isolation off) — `asyncio.sleep`, non-blocking
2. **CPU, no isolation** (isolation off) — SHA-256 busy loop in-process
3. **CPU, with isolation** (isolation on) — SHA-256 busy loop, each job in its own subprocess

Results stored in `loadtest-results/worker-sweep-k6-final/`,
`loadtest-results/sweep-cpu-no-isolation/`, and `loadtest-results/sweep-cpu-isolation/`.

### Three-way comparison (jobs/s · p50)

| Workers | I/O, no isolation | CPU, no isolation | CPU, with isolation |
|---------|-------------------|-------------------|---------------------|
| 1 | 1.87 · 14.1s | 1.23 · 30.2s | 1.63 · 13.0s |
| 2 | 4.03 · 11.1s | 1.03 · 17.0s | 3.33 · 11.0s |
| 3 | 6.00 · 7.7s | 1.50 · 15.6s | 3.30 · 15.4s |
| 4 | 7.87 · 4.2s | 2.03 · 15.3s | 3.80 · 11.4s |
| 5 | **8.03 · 2.6s ← knee** | 2.50 · 14.1s | 5.20 · 10.7s |
| 6 | 8.03 · 2.6s | 2.90 · 13.4s | 5.73 · 8.4s |
| 7 | 8.00 · 2.6s | 3.30 · 12.7s | 6.23 · 6.8s |
| 8 | 8.03 · 2.6s | 3.40 · 12.4s | **8.00 · 5.1s ← knee** |

### Findings

**CPU, no isolation — the GIL problem made visible.**
Each worker has 4 job slots running as coroutines in a single asyncio event loop. The
`await asyncio.sleep(0)` between stages allows cooperative yielding, but only one coroutine
can execute the CPU burn at a time — the GIL serialises them. Effective per-worker throughput
collapses to ~0.43 jobs/s vs ~2 jobs/s in I/O mode. At w=8 the system only delivers
3.4 jobs/s (42% of the 8 jobs/s target), the latency never stabilises, and **the knee does
not appear within 8 workers**. Adding more slots to a worker is nearly useless for CPU-bound
work without isolation.

**CPU, with isolation — GIL bypassed, scaling restored.**
Each slot spawns a subprocess with its own Python interpreter. Subprocesses run on separate
CPU cores, providing true parallel execution. Throughput climbs toward the target and the
knee reappears — but **shifted to w=8** instead of w=5. The extra workers needed reflect the
cost of isolation: subprocess spawn overhead (~300 ms per job on Linux in Docker) and CPU
contention as 32 simultaneous subprocesses (8 workers × 4 slots) share the host's cores,
raising the execution floor from 2.6s (I/O) to 5.1s.

**Design implication.**
For I/O-bound workloads (database queries, API calls, file reads) asyncio coroutines scale
efficiently — isolation adds unnecessary overhead. For CPU-bound workloads (PDF rendering,
large in-process aggregations, hash/crypto operations) isolation is not optional: without it,
the GIL caps per-worker throughput regardless of slot count, and adding workers only partially
compensates. With isolation the scheduler recovers near-linear scaling at the cost of a higher
latency floor and a shifted knee point.
