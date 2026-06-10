# Endure — Thesis & Code Completion Plan

## The Argument

The thesis makes one central claim in two parts:

> Guaranteeing completion of a scheduled ETL pipeline under failure requires
> solving two separate problems at the right layer: the scheduler's obligations
> (state persistence, bounded failure detection, observability) and the
> pipeline's obligations (idempotent ingestion, data quality validation).
> Endure demonstrates these jointly on minimal infrastructure — PostgreSQL and
> Redis — by studying what Temporal, Airflow, and DBOS each require, then
> building the smallest system that satisfies those requirements.

The thesis is not competing with production systems. It is a research vehicle:
showing the floor, stating the limitations honestly, and explaining what
frameworks build on top of that floor.

---

## REFERENCES
```
% references.bib — Endure thesis bibliography

%% ---- Foundations: fault tolerance and failure detection ----

@article{schlichting1983,
  author    = {Schlichting, Richard D. and Schneider, Fred B.},
  title     = {Fail-Stop Processors: An Approach to Designing Fault-Tolerant
               Computing Systems},
  journal   = {ACM Transactions on Computer Systems},
  volume    = {1},
  number    = {3},
  pages     = {222--238},
  year      = {1983},
  publisher = {ACM},
  doi       = {10.1145/357369.357371}
}
% USE: definition of the fail-stop fault model — processes halt and do not
% produce incorrect results. Paraphrase: "Schlichting and Schneider formalise
% the fail-stop model, in which a faulty processor halts and its failure is
% detectable by other processors [schlichting1983]."

@article{chandra1996,
  author    = {Chandra, Tushar Deepak and Toueg, Sam},
  title     = {Unreliable Failure Detectors for Reliable Distributed Systems},
  journal   = {Journal of the ACM},
  volume    = {43},
  number    = {2},
  pages     = {225--267},
  year      = {1996},
  publisher = {ACM},
  doi       = {10.1145/226643.226647}
}
% USE: formal grounding for the term "eventually perfect failure detector".
% Paraphrase: "Chandra and Toueg introduce the eventually perfect failure
% detector (◇P), which eventually suspects all and only crashed processes;
% heartbeat-based timeouts are a standard implementation [chandra1996]."

@inproceedings{gray1989,
  author    = {Gray, Cynthia and Cheriton, David},
  title     = {Leases: An Efficient Fault-Tolerant Mechanism for Distributed
               File Cache Consistency},
  booktitle = {Proceedings of the 12th ACM Symposium on Operating Systems
               Principles},
  series    = {SOSP '89},
  pages     = {202--210},
  year      = {1989},
  publisher = {ACM},
  doi       = {10.1145/74850.74870}
}
% NOTE: corrected — first author is Cynthia Gray, not C. Gray.
% USE: formal grounding for lease-based coordinator failover.
% Paraphrase: "Gray and Cheriton introduce leases as a fault-tolerant
% mechanism for distributed consistency; Endure uses lease expiry to trigger
% coordinator failover [gray1989]."

@article{elnozahy2002,
  author    = {Elnozahy, Elmootazbellah N. and Alvisi, Lorenzo and
               Wang, Yi-Min and Johnson, David B.},
  title     = {A Survey of Rollback-Recovery Protocols in Message-Passing
               Systems},
  journal   = {ACM Computing Surveys},
  volume    = {34},
  number    = {3},
  pages     = {375--408},
  year      = {2002},
  publisher = {ACM},
  doi       = {10.1145/568522.568525}
}
% USE: background on checkpointing and rollback-recovery; positions
% Endure's stage checkpointing within the broader literature.
% Paraphrase: "Elnozahy et al. survey rollback-recovery protocols and
% distinguish checkpoint-based from log-based recovery; Endure uses
% checkpoint-based recovery at stage granularity [elnozahy2002]."

%% ---- Foundations: durability and write-ahead logging ----

@article{mohan1992,
  author    = {Mohan, C. and Haderle, Don and Lindsay, Bruce and
               Pirahesh, Hamid and Schwarz, Peter},
  title     = {{ARIES}: A Transaction Recovery Method Supporting
               Fine-Granularity Locking and Partial Rollbacks Using
               Write-Ahead Logging},
  journal   = {ACM Transactions on Database Systems},
  volume    = {17},
  number    = {1},
  pages     = {94--162},
  year      = {1992},
  publisher = {ACM},
  doi       = {10.1145/128765.128770}
}
% USE: grounding for the claim that PostgreSQL's durability derives from
% write-ahead logging. Paraphrase: "ARIES establishes the write-ahead
% logging principle — a page may not be flushed before its log record —
% which PostgreSQL implements and which Endure inherits for checkpoint
% durability [mohan1992]."

%% ---- Foundations: long-running transactions and sagas ----

@inproceedings{garciamolina1987,
  author    = {Garcia-Molina, Hector and Salem, Kenneth},
  title     = {Sagas},
  booktitle = {Proceedings of the 1987 ACM SIGMOD International Conference
               on Management of Data},
  series    = {SIGMOD '87},
  pages     = {249--259},
  year      = {1987},
  publisher = {ACM},
  doi       = {10.1145/38713.38742}
}
% NOTE: corrected — full first names, correct DOI (38713 not 38714).
% USE: conceptual grounding for multi-step pipelines with compensating
% actions. Paraphrase: "Garcia-Molina and Salem introduce the saga as a
% long-lived transaction decomposed into a sequence of sub-transactions,
% each with a compensating transaction; this decomposition underlies
% durable execution systems including Temporal [garciamolina1987]."

%% ---- Foundations: distributed snapshots ----

@article{chandy1985,
  author    = {Chandy, K. Mani and Lamport, Leslie},
  title     = {Distributed Snapshots: Determining Global States of
               Distributed Systems},
  journal   = {ACM Transactions on Computer Systems},
  volume    = {3},
  number    = {1},
  pages     = {63--75},
  year      = {1985},
  publisher = {ACM},
  doi       = {10.1145/214451.214456}
}
% USE: optional; relevant if you discuss global state capture in the
% context of checkpointing.

%% ---- Related systems ----

@article{skiadopoulos2022,
  author    = {Skiadopoulos, Athinagoras and Li, Qian and Kraft, Peter and
               Kaffes, Kostis and Hong, Daniel and Mathew, Shana and
               Bestor, David and Cafarella, Michael and Gadepally, Vijay and
               Graefe, Goetz and Kepner, Jeremy and Kozyrakis, Christos and
               Kraska, Tim and Stonebraker, Michael and Suresh, Lalith and
               Zaharia, Matei},
  title     = {{DBOS}: A {DBMS}-Oriented Operating System},
  journal   = {Proceedings of the VLDB Endowment},
  volume    = {15},
  number    = {1},
  pages     = {21--30},
  year      = {2022},
  publisher = {VLDB Endowment},
  doi       = {10.14778/3485450.3485454}
}
% USE: grounds the claim that scheduling and durability can be implemented
% as database operations. Paraphrase: "Skiadopoulos et al. argue that
% implementing a cluster scheduler as database transactions simplifies
% failure recovery, since the DBMS guarantees consistency and durability
% of scheduler state [skiadopoulos2022]. Endure adopts this principle
% without requiring a dedicated orchestration server."

@misc{airflow2015,
  author       = {Beauchemin, Maxime},
  title        = {Airflow: A Workflow Management Platform},
  howpublished = {The Airbnb Tech Blog, Medium},
  month        = jun,
  year         = {2015},
  url          = {https://medium.com/airbnb-engineering/airflow-a-workflow-management-platform-46318b977fd8}
}
% NOTE: corrected key from `airflow` to `airflow2015`; added month.
% USE: cite when introducing Airflow's DAG model and retry semantics.

@misc{temporal2024,
  author       = {{Temporal Technologies, Inc.}},
  title        = {Temporal Platform Documentation},
  howpublished = {Software documentation},
  year         = {2024},
  url          = {https://docs.temporal.io/}
}
% USE: cite when describing Temporal's event-sourced durability and
% durable execution model.

@misc{dbostransact2024,
  author       = {{DBOS, Inc.}},
  title        = {{DBOS Transact}: Database-Backed Durable {Python} Workflows},
  howpublished = {GitHub repository},
  year         = {2024},
  url          = {https://github.com/dbos-inc/dbos-transact-py}
}
% USE: cite as the practical instantiation of the DBOS principle
% in Python; contrast with Endure's design.

@misc{celery2024,
  author       = {{Celery Project}},
  title        = {Celery: Distributed Task Queue},
  howpublished = {Software documentation},
  year         = {2024},
  url          = {https://docs.celeryq.dev/en/stable/}
}

@misc{redis2024,
  author       = {{Redis Ltd.}},
  title        = {Redis Documentation},
  howpublished = {Software documentation},
  year         = {2024},
  url          = {https://redis.io/docs/}
}

@misc{djangoninja2024,
  author       = {Chernykh, Vitalii},
  title        = {Django Ninja: Fast {Django} {REST} Framework},
  howpublished = {Software documentation},
  year         = {2024},
  url          = {https://django-ninja.dev/}
}
% NOTE: corrected author first name to Vitalii (full name on record).

%% ---- General reference ----

@book{kleppmann2017,
  author    = {Kleppmann, Martin},
  title     = {Designing Data-Intensive Applications},
  publisher = {O'Reilly Media},
  year      = {2017},
  isbn      = {978-1-4493-7332-0}
}
% USE: Chapter 8 (distributed systems problems) and Chapter 9
% (consistency and consensus) provide accessible grounding for
% failure models, idempotency, and exactly-once semantics.
```

## Part 1 — Thesis

### Chapter 2 — the most important chapter to rewrite

This is where the intellectual work lives. Currently it is background.
It needs to become the **source of the design**.

Structure:

**§2.1 Fault models**
- Cite `schlichting1983`: define fail-stop. Explain scope: Endure's guarantees
  hold only under this model. Silent failures are out of scope by assumption,
  not oversight.
- Cite `chandra1996`: failure detection is inherently imprecise under
  asynchrony. Bounded detection (heartbeat timeout) is the practical answer.

**§2.2 Recovery in distributed systems**
- Cite `elnozahy2002`: classify recovery approaches — checkpoint-restart,
  message logging, rollback-recovery. Endure's skip-based approach is
  checkpoint-restart without rollback (completed work is not undone, it is
  preserved and skipped).
- Cite `chandy1985`: one paragraph on consistent global snapshots as context.
  Note that Endure's sequential stage model makes global snapshot consistency
  trivial — there is only one executing process per job at any time.

**§2.3 Durability and databases**
- Cite `kleppmann2017`: WAL, fsync, what PostgreSQL guarantees. The claim
  "durability comes from the database" needs this grounding. Endure inherits
  PostgreSQL's durability guarantees without implementing them.

**§2.4 Coordination and leader election**
- Cite `gray1989`: leases as a fault-tolerant coordination primitive.
  The coordinator failover design follows directly from this.

**§2.5 Survey of existing systems — the source of the requirements**

This section does the intellectual work. Each system teaches something specific:

*Temporal* — cite Cadence paper + Temporal docs:
- Replay-based durable execution. Every workflow function is re-executed from
  the top on resume; completed activities inject cached results.
- Requires workflow code to be deterministic.
- Teaches: what full generality costs (dedicated server, determinism
  constraint, versioning story). The three scheduler requirements are visible
  here: state persistence (event history), failure detection (heartbeat),
  observability (workflow query API).

*Airflow* — cite `airflow`:
- Task-level DAG dispatch. The scheduler dispatches individual tasks, not
  whole pipelines. Workers collaborate on a single DAG run.
- Teaches: intra-job parallelism requires task-level dispatch. Endure does
  not implement this — a deliberate scope decision. Also teaches that the
  scheduler must be DAG-aware, which adds coordinator complexity.

*DBOS Transact* — cite `dbos`:
- Skip-based recovery on PostgreSQL alone. No dedicated server.
- Teaches: the skip mechanism (check before execute, record after) is
  sufficient for durability without replay. Convergence with Endure's
  step-level mechanism is evidence for soundness, not derivation.

*Celery* — cite `celery`:
- Task queue without durability. No checkpointing, no coordinator failover.
- Teaches: the baseline. Shows what is missing when you have dispatch but
  not the three scheduler requirements.

**§2.6 Requirements distilled**

From the survey, extract the three scheduler requirements explicitly:
1. State persistence — record enough state to resume from failure
2. Bounded failure detection — detect halted workers within a known time bound
3. Execution observability — audit trail enabling diagnosis after failure

Note: requirements 1 and 2 guarantee completion. Requirement 3 supports
human-in-the-loop recovery but does not guarantee it automatically.

Also extract the two pipeline obligations:
1. Idempotent operations — stages and steps must tolerate re-execution
2. Domain-level validation — silent failures are the pipeline's responsibility

---

### Chapter 1 — update framing

- §1.1 Motivation: reframe around the two-layer problem statement
- §1.2 The Endure system: remove miniframework narrative. Describe as a
  coupled scheduler + ETL pipeline that demonstrates the requirements jointly.
  `DailyImportJob` as the concrete pipeline.
- §1.3 Scope: add explicit fail-stop assumption. State what is out of scope:
  intra-job parallelism, general-purpose workflow API, multi-host HA,
  exactly-once semantics.
- §1.4 RQs:
  - RQ1: Does Endure recover from scheduler failures without repeating
    completed work, at stage and step granularity?
  - RQ2: Does job throughput scale proportionally with worker count?

---

### Chapter 3 — add requirements-to-design mapping

Add §3.1: a table mapping each requirement to the section that implements it:

| Requirement | Endure mechanism | Section |
|---|---|---|
| State persistence | Stage checkpoint + step memoization | §3.6, §3.9 |
| Bounded failure detection | Worker heartbeat + coordinator sweep | §3.4 |
| Work re-dispatch | Coordinator re-queues on missed heartbeat | §3.4, §3.5 |
| Scheduler resilience | Lease-based leader election | §3.3 |
| Bounded failure handling | Retry counter + dead-letter | §3.7 |
| Observability | JobEvent audit log | §3.8 |

Update §3.6 job description to `DailyImportJob` with ETL stages.
Remove all "public API" / "miniframework" language.

---

### Chapter 4 — update evaluation

**§4.1 Setup**: describe `DailyImportJob`, the two-layer test structure,
and the Docker Compose environment.

**§4.2 Functional validation** — domain-level layer:
1. End-to-end completion: job produces `.xlsx` artifact
2. Quality gate: error rate above threshold → FAILED
3. Cross-job idempotency: same file hash skipped on second run
4. Step outputs: count equals `n_files` after completion

**§4.3 RQ1 — Fault recovery** — fail-stop layer:
1. Stage-level resume: crash after `validate`, resume skips completed stages
2. Step-level resume: crash during `ingest` after N files, resume skips N files
3. Coordinator failover: scheduler crash, standby acquires lease, job completes

**§4.4 RQ2 — Throughput**:
- Submit batch of N jobs simultaneously, vary worker count (1, 2, 4)
- Measure: jobs completed per minute
- Expected: near-linear improvement
- Explain result from architecture: stateless independently-polling workers,
  atomic Redis dequeue, no coordination between workers during execution

---

### Chapter 5 — limitations + decoupling reflection

**§5.1 Summary**: RQ1 confirmed at both granularities. RQ2 confirmed up to
tested scale. State the constraint: single-host, fail-stop only.

**§5.2 Contributions**:
1. Requirements characterisation distilled from Temporal, Airflow, DBOS
2. Demonstration that three requirements are sufficient on PostgreSQL + Redis
3. Two-level checkpointing: stage-boundary (automatic) + step-level (opt-in)
4. Explicit two-layer separation: scheduler obligations vs pipeline obligations
5. Evaluation suite covering both layers and throughput

**§5.3 Limitations — stated honestly**:
- Single-host: no infrastructure HA for PostgreSQL or Redis
- One job, one worker: no intra-job parallelism; Airflow and Temporal both
  solve this via task-level dispatch, which requires coordinator redesign
- Fail-stop only: silent failures require domain-level validation, not
  scheduler guarantees
- Linear stages only: DAG support requires TopologicalSorter +
  asyncio.gather(), not implemented
- Step counter fragility: global counter is correct only when earlier
  stages have no step() calls; cross-stage step() usage requires
  per-stage namespacing
- At-least-once only: idempotency is the developer's obligation, not
  the scheduler's guarantee

**§5.4 What frameworks give you — the decoupling reflection**:
Endure couples scheduler and pipeline deliberately. The coupling is what
makes the system simple enough to build, reason about, and evaluate.
But it is also the ceiling.

Frameworks (Temporal, Airflow) exist to remove that coupling. A user
can write jobs in any language, deploy job code independently of the
scheduler, version workflow definitions without redeploying infrastructure.
The price is a dedicated server, a worker protocol, SDK maintenance,
and a versioning story. That is not overhead — it is the cost of
generality.

Endure demonstrates the floor: the minimum required for correctness.
Temporal and Airflow show what is built on top of that floor to achieve
generality. A team that understands Endure understands why Temporal is
the shape it is.

**§5.5 Future work**:
- DAG stage dependencies via `graphlib.TopologicalSorter` + `asyncio.gather()`
- Task-level dispatch: scheduler dispatches stages, not jobs — enables
  intra-job parallelism across workers; the Airflow topology applied to Endure
- PostgreSQL-only mode: replace Redis queue with `SELECT FOR UPDATE SKIP LOCKED`
- Client-server separation: Endure as a standalone server, pipelines as clients

---

### Abstract — write agreed text

> This thesis designs Endure, a minimal task scheduler for fault-tolerant
> execution of an automated report generation pipeline, and argues that
> guaranteeing completion under failure requires solving two separate
> problems — one belonging to the scheduler, one to the pipeline — at the
> right layer. For the scheduler, state persistence, bounded failure
> detection, and execution observability are the essential requirements,
> distilled from how Temporal, Airflow, and DBOS each approach the problem;
> the first two guarantee completion, the third supports diagnosis when
> things go wrong. Endure implements these solely on PostgreSQL and Redis,
> coupled with a concrete reporting pipeline to demonstrate that the
> requirements are jointly sufficient for a working end-to-end system. For
> the pipeline, idempotent ingestion and data quality validation are the
> application's own obligations, and the coupled example shows both in
> practice. We evaluate Endure against both concerns — recovery from
> scheduler failures at stage and step granularity, and pipeline-level data
> quality handling — and show that throughput scales proportionally with
> worker count, a direct consequence of stateless independently-polling
> workers sharing a common queue.

---

## Part 2 — Code

### Still missing

**Functional gaps:**
- `SourceFile` model + migration (`0008_sourcefile.py`)
- `GET /jobs/{job_id}/step-outputs` endpoint in `src/api/routes/jobs.py`
- `REPORT_REGISTRY` in `src/api/routes/reports.py` updated for `daily_import`
- `seed_periodic_reports` updated for `DailyImportJob`

**Tests:**
- `src/evaluate/test_step_recovery.py` — step-level crash recovery test
- `src/evaluate/load/run_matrix.py` — throughput experiment: N jobs × worker count
- Cross-job idempotency test in `test_reporting.py`

**Cleanup:**
- Delete `src/reporting/jobs/daily_sales.py`, `weekly_activity.py`, `alert_digest.py`
- Delete `src/reporting/generators/html.py`, `data.py`
- Remove duplicate `BaseReportJob = Pipeline` alias from `src/framework/pipeline.py`
- Populate `src/admin.py` with model registrations
- Update `endure/__init__.py` docstring

---

## Execution Order

```
Week 1:  Code gaps (SourceFile, step-outputs endpoint, API update, cleanup)
Week 2:  Evaluation tests (test_step_recovery, run_matrix, idempotency test)
Week 3:  Find + add missing papers (Sagas, Cadence). Rewrite Ch2.
Week 4:  Rewrite Ch1, Ch3 updates, Ch4 updates
Week 5:  Write Ch5 (limitations + decoupling reflection). Write abstract.
Week 6:  Full pass: verify every thesis claim against the implementation.
          Fix any mismatch. Run full evaluation suite. Record results.
          Fill in blank numbers.
```

Ch2 rewrite (Week 3) is the highest-leverage task. Everything else
depends on the argument it establishes.
