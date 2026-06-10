# Endure — Implementation & Thesis Plan

## Context

The thesis framing has pivoted from "we built a miniframework" to:

> We characterise the properties a task scheduler must exhibit to guarantee completion
> of scheduled ETL pipelines under fail-stop failures, and demonstrate a
> minimal-infrastructure implementation that satisfies them.

Five design-agnostic requirements (any correct system must address these, not
necessarily the same way):

1. **Durability** — record enough state to resume from the point of failure, not from scratch
2. **Failure detection** — detect worker failure within a bounded time
3. **Work re-dispatch** — detected failures trigger reassignment of incomplete work
4. **Scheduler resilience** — the coordinator must not be a single point of failure
5. **Bounded failure handling** — repeated failures converge to a terminal state, not infinite retry

Endure's specific answers: skip-based stage checkpointing + step-level memoization,
heartbeat monitoring, coordinator lease election, dead-letter quarantine —
all on PostgreSQL + Redis, no dedicated orchestration server.

---

## Part 1 — Code

### 1A · Codebase cleanup

**Register models in Django admin:**
- Create a real `src/admin.py` that registers `Job`, `Tenant`, `Worker`,
  `Checkpoint`, `StepOutput`, `PeriodicTask`, `DeadLetterJob`, `SourceFile`
  (once that model exists). Useful during development and thesis demos.

**`src/framework/pipeline.py`:**
- Remove the `BaseReportJob = Pipeline` alias at the bottom — it already
  lives in `src/reporting/jobs/base.py`. Having it in two places is confusing.

**`endure/__init__.py`:**
- Edit  a module docstring to mention that this is an internal import convenience. Remove the usage example that implies external users.

**`src/reporting/generators/html.py`:**
- Keep for now but mark as superseded by Excel output (Phase 1C). Remove
  once `DailyImportJob` is the primary job and HTML is no longer used.

**`src/evaluate/test_reporting.py` comments:**
- Already patched (`stage_validate` → `validate stage`). No further action.

---

### 1B · New evaluation job: `DailyImportJob`  _(~2 days)_

Replaces `DailySalesReportJob`, `WeeklyActivityReportJob`, `AlertDigestReportJob`.
This is the primary job used in all evaluation scenarios.

**Motivation for replacing the existing jobs:**
The existing jobs generate synthetic in-memory data and produce HTML output.
The thesis is now about ETL pipelines. `DailyImportJob` simulates a realistic
ETL load: discovering CSV files in a landing directory, ingesting them one at a
time (with one `step()` call per file), validating, transforming, and producing
an Excel output. The per-file `step()` story is the central evaluation
narrative: "the job crashed after ingesting 12 of 20 files; on resume it skipped
those 12 and processed the remaining 8."

**Stages:**

```
discover → ingest → validate → transform → report → archive
```

- `discover` — list synthetic CSV files in the landing directory; filter out
  files already recorded in `SourceFile` (cross-job idempotency). Returns
  `{"files": [...], "file_count": N}`.

- `ingest` — iterate `state["files"]`; for each file call
  `await step(f"file_{i}", _read_csv, path)`. One step per file. Crash
  mid-ingest resumes from the next unprocessed file. Returns
  `{"records": [...], "total_records": N}`.

- `validate` — null checks, duplicate IDs, negative quantities, invalid
  product codes, row-count anomaly (actual vs expected). Returns
  `{"quality": {...}, "valid_records": [...], "error_rows": [...]}`.

- `transform` — date normalization (ISO-8601), column name standardization
  (snake_case), numeric type enforcement. Returns `{"transformed": [...]}`.

- `report` — generate `.xlsx` via openpyxl: Sheet 1 = daily quality summary
  (totals, pass/fail per check); Sheet 2 = exception detail (error rows with
  source file reference). Returns `{"xlsx_bytes": ..., "summary": {...}}`.

- `archive` — save `.xlsx` to `REPORT_OUTPUT_DIR`; write `SourceFile` rows
  marking each processed file. Returns `{"artifact_path": "..."}`.

**New files:**
- `src/reporting/jobs/daily_import.py` — `DailyImportJob` implementation
- `src/reporting/generators/csv_data.py` — synthetic CSV generator; produces
  deterministic files keyed on `(seed, n_files, rows_per_file)`; supports
  injecting error rows (nulls, duplicates, negative quantities, bad codes)
- `src/reporting/generators/excel.py` — openpyxl Excel renderer; two-sheet
  workbook (quality summary + exception detail)

**Modified files:**
- `src/reporting/storage.py` — extend `save_artifact` to accept `bytes`
  (for Excel) in addition to `str` (for legacy HTML); auto-select extension
  from content type or explicit `ext` parameter
- `src/api/routes/reports.py` — replace `REPORT_REGISTRY` entries with
  `"daily_import": "src.reporting.jobs.daily_import:DailyImportJob"`; update
  `ReportSubmitRequest.report_type` Literal
- `src/management/commands/seed_periodic_reports.py` — replace the three
  existing task definitions with one `DailyImportJob` task (cron: `0 6 * * *`)

**Payload fields:**
```
tenant_id     str   — tenant identifier
date          str   — ISO-8601 date [default: today]
n_files       int   — number of synthetic CSV files to generate [default: 20]
rows_per_file int   — rows per file [default: 500]
seed          int   — RNG seed [default: 42]
inject_errors int   — number of error rows to inject across files [default: 5]
```

**Dependencies to add (`pyproject.toml`):**
- `openpyxl>=3.1`

---

### 1C · `SourceFile` model  _(~half a day)_

Cross-job idempotency: if the same file is re-sent unchanged, `discover` skips
it. If it arrives with corrected records (different hash), `discover` picks it
up as a new file.

**New model `src/models.py`:**
```python
class SourceFile(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE)
    file_name = models.CharField(max_length=512)
    file_hash = models.CharField(max_length=64)   # SHA-256 hex
    job = models.ForeignKey(Job, on_delete=models.SET_NULL, null=True)
    processed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("tenant", "file_hash")]
        db_table = "source_files"
```

**New migration:** `src/migrations/0008_sourcefile.py`

**Usage in `discover`:** query `SourceFile.objects.filter(tenant_id=..., file_name__in=candidates)` and exclude files whose current hash is already recorded. New or corrected files pass through.

---

### 1D · API: `GET /jobs/{job_id}/step-outputs`  _(~2 hours)_

Referenced in thesis §4.3 evaluation scenarios. Evaluation tests poll this
endpoint to know when the first step has completed before injecting a fault.

**Add to `src/api/routes/jobs.py`:**
```python
@router.get("/{job_id}/step-outputs")
async def get_step_outputs(request, job_id: uuid.UUID):
    # 404 if job not found
    # return list of {step_id, step_name, created_at} ordered by step_id
    # do not return output field (can be large; callers only need to know completion)
```

---

### 1E · Remove old report jobs  _(~1 hour)_

Once `DailyImportJob` is working and all evaluation tests pass:
- Delete `src/reporting/jobs/daily_sales.py`
- Delete `src/reporting/jobs/weekly_activity.py`
- Delete `src/reporting/jobs/alert_digest.py`
- Delete `src/reporting/generators/html.py` (no longer used)
- Delete or archive `src/reporting/generators/data.py` (replace with `csv_data.py`)

Keep:
- `src/reporting/jobs/base.py` (`BaseReportJob = Pipeline` alias)
- `src/reporting/storage.py` (updated for Excel)
- `src/evaluate/jobs.py` (`SyntheticJob` — used by scalability benchmarks)

---

### 1F · Evaluation tests  _(~1 day)_

**`src/evaluate/test_reporting.py`** — rewrite for `DailyImportJob`:
- Submit a `DailyImportJob` with `n_files=5, rows_per_file=100`
- Assert COMPLETED, artifact_path ends in `.xlsx`, quality summary present
- Assert data quality: submit with `inject_errors=50` (beyond threshold), assert FAILED or DEAD_LETTER
- Remove all references to `daily_sales`, `weekly_activity`, `alert_digest`

**`src/evaluate/test_step_recovery.py`** — new :
- Submit `DailyImportJob` with `n_files=10, rows_per_file=200`
- Wait until `GET /jobs/{id}/step-outputs` returns at least 3 entries (3 files ingested)
- Kill the assigned worker container (`docker kill endure-worker-1`)
- Wait for coordinator to re-queue and a new worker to pick up
- Assert: state = COMPLETED, step_outputs count = 10 (all files), audit log contains skip events

**Keep unchanged:**
- `test_checkpoint.py` — uses `SyntheticJob`, tests stage-level resume; still valid
- `test_chaos_recovery.py` — uses `SyntheticJob`; still valid
- `test_dlq.py` — uses `SyntheticJob`; still valid
- `test_job_lifecycle.py` — generic lifecycle; still valid
- `test_periodic_tasks.py` — tests PeriodicTask scheduling; still valid
- `test_process_isolation.py` — uses `SyntheticJob`; still valid
- `test_leader_metrics.py` — scheduler failover; still valid

---

## Part 2 — Thesis

### Abstract 

Durable execution, popularized by Temporal, was designed for transactional workflows where no step may fail: payments, order fulfilment, distributed sagas. It has since found new application in data pipelines and AI agent workflows. Traditional task schedulers addressed long-running processes and later DAGs, but did not guarantee completion under failure. For long-running, multi-step pipelines with time constraints — such as automated reporting that must land before business hours — durable execution is the right lens.

This thesis characterises the properties a task scheduler must exhibit to guarantee completion of scheduled ETL pipelines under fail-stop failures, and demonstrates a minimal-infrastructure implementation that satisfies them. We identify five requirements any such system must address — durability of progress, timely failure detection, work re-dispatch, scheduler resilience, and bounded failure handling — and demonstrate one minimal-infrastructure design that satisfies them: skip-based stage checkpointing, heartbeat monitoring, lease-based coordinator failover, and dead-letter quarantine, using PostgreSQL and Redis alone, without a dedicated orchestration server. We evaluate Endure against two questions: does it recover correctly from worker and coordinator failures without repeating completed work, and does throughput scale predictably as worker count and data volume increase.

— write to `thesis/abstract.tex`.

---

### Chapter 1 — Introduction

**§1.2 The Endure System:**
- Remove: miniframework narrative, "from endure import Pipeline, step" as
  a public API claim, "third problem: step() function as framework contribution"
- Replace with: Endure is a coupled reporting + scheduling system that
  demonstrates all five requirements in a single deployable unit. `Pipeline`
  and `step()` are internal abstractions — not a public SDK, so remove them. The coupling is
  a deliberate design choice that lets us show how the requirements emerge
  from domain constraints.
- Update the concrete job description: one primary job (`DailyImportJob`),
  ETL-shaped stages, per-file step() calls.

**§1.3 Scope and Claims:**
- Remove: "miniframework with two public exports"
- Replace: the system claims to satisfy all five requirements; the evaluation
  verifies each. Explicitly out of scope: general-purpose workflow API,
  language-agnostic worker protocol, PII/RBAC, multi-host infrastructure HA.

**§1.4 Research Questions:**
- RQ1 wording: "Does Endure recover and produce correct output without
  repeating completed work, at both stage and step granularity?"
- RQ2 wording: unchanged (throughput scalability with workers)
- Add a sentence connecting RQs to requirements: RQ1 tests requirements 1–3,
  RQ2 tests whether the architecture (requirement 4 + worker design) scales.

---

### Chapter 2 — Background

**Minimal changes:**
- Update the six-column comparison table: replace "Endure" row's Domain cell
  from "Reporting/ETL" to "Scheduled ETL" for precision
- Update the positioning paragraph: remove "miniframework emergence" language;
  replace with "Endure begins from a specific domain problem and arrives at
  a durable execution design — a design-specific instance of the general
  pattern independently used by others ex: DBOS Transact"
- No structural changes needed

---

### Chapter 3 — Design

**Add §3.1 Requirements to Design Mapping** (new, ~1 page):
A table mapping each of the five requirements to the section(s) that describe
how Endure satisfies it. This makes the chapter read as answers to requirements
rather than a feature tour:

| Requirement | Endure mechanism | Section |
|---|---|---|
| Durability | Stage checkpointing + step memoization | §3.6, §3.9 |
| Failure detection | Worker heartbeat + coordinator sweep | §3.4 |
| Work re-dispatch | Coordinator re-queues on missed heartbeat | §3.4, §3.5 |
| Scheduler resilience | Lease-based leader election | §3.3 |
| Bounded failure | Retry counter + dead-letter | §3.7 |

**§3.6 Pipeline Stage Design:**
- Update job description to `DailyImportJob` with ETL stages
- Drop "public API" language; describe `Pipeline` as the internal base class
  that enforces the stage contract (static declaration, idempotency requirement)
- Keep the code listing but update it to show `DailyImportJob.ingest` with
  per-file `step()` calls

**§3.9 Step-level Checkpointing:**
- Keep the mechanism description (contextvar, StepOutput table, skip logic)
- Drop "miniframework contribution" framing; describe as "an opt-in mechanism
  that reduces recovery granularity from stage-boundary to individual
  sub-operation"
- Update the example: file ingestion (`step("file_3", read_csv, path)`) is
  more concrete than the in-memory aggregation example

**Data model table:**
- Add `source_files` row

**Remove:**
- Any remaining references to `from endure import Pipeline, step` as a
  user-facing API in §3.6 or §3.9

---

### Chapter 4 — Evaluation

**§4.1 Setup:**
- Replace description of `DailySalesReportJob` with `DailyImportJob`
- Describe the per-file `step()` pattern: N files, each ingested as one step,
  enabling step-level crash recovery mid-ingest
- Keep `SyntheticJob` description for the scalability benchmark

**§4.2 Functional Validation table** — 6 rows:
1. End-to-end: `DailyImportJob` completes, `.xlsx` artifact produced
2. Data quality: inject errors beyond threshold → job fails with quality error
3. Idempotency: re-submit same file hash → file skipped in `discover`
4. Worker crash → stage-level resume (via `SyntheticJob` or `DailyImportJob`)
5. Worker crash → step-level resume: 12/20 files ingested, crash, resume skips 12
6. Scheduler failover → job completes after coordinator handover

**§4.3 RQ1 Fault Recovery:**
- Scenario 1: worker crash at stage boundary — unchanged
- Scenario 2: worker crash mid-ingest (step-level) — rewrite using
  per-file steps; "12 of 20 files ingested at crash; 12 skipped on resume"
  is a clearer story than "group_by_product completed, apply_fx did not"
- Scenario 3: scheduler failover — unchanged

**§4.4 RQ2 Scalability:**
- No changes

**§4.5 Threats to Validity:**
- Update step-level threat: "step-level checkpointing reduces but does not
  eliminate repeated work; a file read that crashes mid-execution restarts
  from its beginning, so the read function must be idempotent"

---

### Chapter 5 — Conclusion

**§5.2 Contributions — replace with:**
1. A characterisation of five design-agnostic requirements for durable
   execution of scheduled ETL pipelines
2. A demonstration that all five can be satisfied with minimal infrastructure
   (PostgreSQL + Redis, no dedicated orchestration server) in a single
   deployable system
3. A two-level checkpointing design: stage-boundary (automatic, no
   determinism constraint) and step-level (opt-in, per sub-operation),
   applied to a realistic ETL load
4. An evaluation suite covering fault recovery at both granularities and
   throughput scalability

Remove: "miniframework API (`from endure import Pipeline, step`)" as a
contribution bullet.

**§5.3 Reflection:**
- Remove the miniframework emergence paragraph
- Replace with: the coupling of reporting and scheduling is not a limitation
  but a deliberate scope — it allows each requirement to be implemented with
  full knowledge of the domain structure (static stage order, idempotent
  operations, known output schema), which a general-purpose framework cannot
  assume. The trade-off is clear: Endure cannot schedule arbitrary workflows,
  but within its domain it makes stronger guarantees with less mechanism.

**§5.4 Future Work:**
- Remove: "pip-installable miniframework SDK"
- Keep: PostgreSQL-only mode (replace Redis queue with `SELECT FOR UPDATE SKIP LOCKED`)
- Keep: DAG stage dependencies (parallel stages via `asyncio.gather`)
- Add: client-server separation (Endure as a standalone server, reporting
  workers as clients — the Temporal topology applied to this domain)
- Add: cross-job file deduplication via `SourceFile` manifest is already
  implemented; a natural extension is a full data lineage graph (report → 
  transformed records → source files)

---

## Part 3 — Execution Order

```
Phase 1     Codebase cleanup (1A)
Phase 2     DailyImportJob + CSV generator + Excel output (1B)
Phase 3     SourceFile model + migration (1C)
Phase 4     GET /step-outputs endpoint (1D)
Phase 5     Evaluation tests rewrite (1F)
Phase 6     Remove old jobs (1E) — only after Phase 5 passes
Phase 7     Thesis rewrite (Ch1, Ch3, Ch5; Ch2, Ch4 lighter)
```

Phases 1–6 can be done before touching the thesis.
Phase 7 can overlap with Phases 3–6 for Ch1 and Ch5 (which don't depend
on the new job implementation).

---

## other isuues to note before starting

1. **Landing directory**: ynthetic CSV files to be written to a
   temp directory at job start time, generate at `discover` time, store
   in a temp dir keyed on `(tenant_id, date, seed)`.

2. **Excel output **: write the file in the `archive` stage and
   only store the path. Ensure `report` returns a serializable summary dict,
   not the raw bytes.

3. **Old report jobs in git history**: deleting them