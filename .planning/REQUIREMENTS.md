# Requirements: Lightweight Airflow CSV→Oracle ETL Platform

**Defined:** 2026-09-01
**Milestone:** v1.1 — Hourly Ingestion Automation
**Core Value:** A single HTTP request can trigger an Airflow DAG that reads a generated CSV,
validates and bulk-loads its valid rows into Oracle, routes invalid rows (with error metadata)
into a separate table, and reports back a clear processing summary — end to end, reproducibly,
from a fresh `git clone`.

## v1.1 Requirements

Requirements for this milestone. Each maps to roadmap phases.

### Environment (ENV)

- [x] **ENV-01**: `generator/generate_csv.py` runs inside the Airflow container — mounted at
      `/opt/airflow/generator`, importable via an extended `PYTHONPATH`, with `faker==40.37.0`
      installed in the image (exact match to the version already pinned in root
      `pyproject.toml`/`uv.lock`)
- [x] **ENV-02**: The Airflow container can write generated CSVs into `data/<dataset>/` on a
      genuinely fresh clone — `data/` gets a write-capable permission fix via a compose-level
      `airflow-init` chown step, not a manual host-side fix
- [x] **ENV-03** (added during Phase 8 planning, D-04 bundled scope addition): The
      `docker/airflow/simple_auth_manager_passwords.json.generated` bind-mount-becomes-directory
      gotcha is repaired proactively by the same compose-level `airflow-init` root-user mechanism
      as ENV-02 — idempotently, on every `docker compose up`, not left as a documented manual
      `chmod 666` step

### Scheduling / Orchestrator DAG (SCHED)

- [x] **SCHED-01**: A new `csv_generate_schedule` DAG runs automatically every hour
      (`schedule="@hourly"`, `catchup=False`), with no manual `make generate` step required
- [x] **SCHED-02**: Each hourly run generates a fresh, non-duplicate customers+orders CSV pair —
      the generation seed varies per run so checksums differ hour to hour, avoiding a silent
      idempotency no-op at the Oracle ingestion layer
- [x] **SCHED-03**: Each hourly run sequentially triggers `csv_ingest` for customers, then orders,
      then `report_ready`, waiting for each to actually finish before the next step starts
      (customers must fully commit before orders, per the Phase 7 FK-existence DB trigger)
- [x] **SCHED-04**: Only one hourly cycle runs at a time (`max_active_runs=1` on the new DAG),
      preventing an overrunning cycle from racing the next scheduled cycle over the same day's CSV
      filename
- [x] **SCHED-05**: If `csv_ingest` or `report_ready` is ever manually paused, the hourly cycle
      fails loudly and immediately instead of hanging (`fail_when_dag_is_paused=True`)
- [x] **SCHED-06**: `csv_ingest.py` and `report_ready.py` remain completely unmodified and
      independently triggerable (e.g. still directly usable from the Airflow UI or REST API, as
      before)
- [x] **SCHED-07**: Each hourly cycle logs a one-line cascade summary (dataset row counts,
      report-ready status) at the parent DAG level — log-only, matching the existing
      `report_result_task`/`build_report_task` convention
- [x] **SCHED-08**: An operator can configure customers/orders row counts and invalid-ratio for a
      scheduled cycle via DAG `Param`s instead of editing code
- [x] **SCHED-10** (added during Phase 9 discuss-phase, D-15 bundled scope addition): A retention
      task inside `csv_generate_schedule` deletes generated CSVs (`.csv`/`.csv.gz`) older than 30
      days from `data/customers/`/`data/orders/`, running at the end of each hourly cascade,
      best-effort (a cleanup failure never fails the overall DagRun)

### Robustness (ROBUST)

- [ ] **ROBUST-01**: A transient Oracle connectivity error during `report_ready`'s polling no
      longer permanently crashes the deferred sensor — `OraclePartitionReadyTrigger.run()` catches
      `oracledb.OperationalError` specifically with bounded retry/backoff; a genuine non-transient
      error (e.g. a bad query) still surfaces as a visible failure

## Future Requirements

Deferred to a future release. Tracked but not in this milestone's roadmap.

### Scheduling

- **SCHED-09**: Reconcile `csv_ingest`'s `wait_for_file` `FileSensor` timeout (3600s) being
  numerically equal to the new hourly schedule period — a real structural tightness with no
  comfortable margin, flagged by research but out of this milestone's scope since fixing it would
  require modifying `csv_ingest.py`, which SCHED-06 requires to stay unmodified this milestone

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| `reset_dag_run=True` on any `TriggerDagRunOperator` | Documented upstream Airflow bug (apache/airflow#57756) when combined with `deferrable=True` and an explicit `trigger_run_id` — unneeded here since `trigger_run_id` stays unset in the default/happy path (no `DagRunAlreadyExists` case ever arises) |
| A deterministic/derived `trigger_run_id` "for idempotency" | Solves an already-solved problem — `csv_processor`'s checksum-keyed idempotency (`ingestion_metadata`, v1.0 Phase 4) already makes a duplicate trigger a safe no-op one layer down. **Narrowed by Phase 9 CONTEXT.md D-06:** this row is about *data* idempotency (re-ingesting the same content), a different concern from D-06's deterministic `trigger_run_id` (`"{{ dag_run.run_id }}__customers"` etc.), which exists solely to make a *retried Airflow task* target the same child DagRun instead of orphaning a duplicate — fixing PITFALLS.md Pitfall 7's retry-orphan finding, not attempting data-level idempotency this row already covers |
| Fan-out/parallel triggering of `csv_ingest` for customers and orders | Phase 7's DB-level `BEFORE INSERT` FK-existence trigger on `orders_valid` requires `customers_valid` to be populated first — must stay strictly sequential |
| Modifying `csv_ingest.py`/`report_ready.py` to add overlap guards or new params | This milestone requires both to remain unmodified and independently triggerable; all sequencing/overlap-prevention is achievable entirely from the new parent DAG's own parameters (SCHED-03/SCHED-04) |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| ENV-01 | Phase 8 | Complete |
| ENV-02 | Phase 8 | Complete |
| ENV-03 | Phase 8 | Complete |
| SCHED-01 | Phase 9 | Complete |
| SCHED-02 | Phase 9 | Complete |
| SCHED-03 | Phase 9 | Complete |
| SCHED-04 | Phase 9 | Complete |
| SCHED-05 | Phase 9 | Complete |
| SCHED-06 | Phase 9 | Complete |
| SCHED-07 | Phase 9 | Complete |
| SCHED-08 | Phase 9 | Complete |
| SCHED-10 | Phase 9 | Complete |
| ROBUST-01 | Phase 10 | Pending |

**Coverage:**
- v1.1 requirements: 13 total (11 original + ENV-03 added during Phase 8 planning per CONTEXT.md
  D-04's user-approved bundled scope addition + SCHED-10 added during Phase 9 discuss-phase per
  CONTEXT.md D-15's user-approved bundled scope addition)
- Mapped to phases: 12
- Unmapped: 0 ✓

---
*Requirements defined: 2026-09-01*
*Last updated: 2026-09-01 after ROADMAP.md creation (Phases 8-10, 100% coverage)*
