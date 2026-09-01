# Roadmap: Lightweight Airflow CSV→Oracle ETL Platform

## Milestones

- ✅ **v1.0 MVP** — Phases 1-7 (shipped 2026-08-30)
- 🚧 **v1.1 Hourly Ingestion Automation** — Phases 8-10 (in progress)

## Phases

<details>
<summary>✅ v1.0 MVP (Phases 1-7) — SHIPPED 2026-08-30</summary>

- [x] Phase 1: Environment & Oracle Foundation (5/5 plans) — completed 2026-08-28
- [x] Phase 2: Config Contract & CSV Generator (5/5 plans) — completed 2026-08-29
- [x] Phase 3: CSV Processing Engine (10/10 plans) — completed 2026-08-29
- [x] Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint (3/3 plans) — completed 2026-08-29
- [x] Phase 5: Airflow DAG Wiring & Deferrable File Wait (2/2 plans) — completed 2026-08-29
- [x] Phase 6: End-to-End Verification, Benchmark, CI & Docs (5/5 plans) — completed 2026-08-30
- [x] Phase 7: Correlated Customer-Order Business Report (6/6 plans) — completed 2026-08-30

Full phase detail: `.planning/milestones/v1.0-ROADMAP.md`
Requirements traceability: `.planning/milestones/v1.0-REQUIREMENTS.md`
Retrospective: `.planning/RETROSPECTIVE.md`

</details>

### 🚧 v1.1 Hourly Ingestion Automation (In Progress)

**Milestone Goal:** Automate the CSV → Oracle pipeline end-to-end on an hourly cadence, with no
manual `make generate` step.

- [x] **Phase 8: Environment & Docker Fixes for Container-Side Generation** - The Airflow container (completed 2026-09-01)
      can import the generator and faker, and can write generated CSVs into `data/<dataset>/` on a
      genuinely fresh clone
- [ ] **Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`)** - A new hourly DAG generates
      fresh CSVs and chain-triggers `csv_ingest` (customers → orders) then `report_ready`, fully
      unattended
- [ ] **Phase 10: `OraclePartitionReadyTrigger` Robustness Fix** - A transient Oracle error during
      `report_ready`'s polling no longer permanently crashes the deferred sensor

## Phase Details

### Phase 8: Environment & Docker Fixes for Container-Side Generation
**Goal**: The Airflow container has everything it needs to generate CSVs in-process and write them
to `data/<dataset>/`, proven independently before any DAG code depends on it.
**Depends on**: Phase 7 (v1.0, shipped)
**Requirements**: ENV-01, ENV-02, ENV-03
**Success Criteria** (what must be TRUE):
  1. A freshly rebuilt Airflow container image has `faker==40.37.0` installed (exact match to root
     `pyproject.toml`/`uv.lock`) and can `import generator.generate_csv` via the mounted
     `/opt/airflow/generator` path and extended `PYTHONPATH` — verified by an actual manual exec
     check inside the container, not just a successful `docker compose build`
  2. On a genuinely fresh clone (`make destroy && make up`), the `airflow-init` step chowns `data/`
     so the running Airflow container (`uid 50000:gid 0`) can create new files under
     `data/customers/` and `data/orders/` without any manual host-side `chmod`/`mkdir` step
  3. Re-running `make up` against an already-initialized `data/` directory does not fail or error —
     the `airflow-init` chown step is idempotent and gated by the existing `depends_on:
     service_completed_successfully` chain
**Plans**: 2 plans
Plans:
- [x] 08-01-PLAN.md — Docker environment foundation: generator/ mount + PYTHONPATH extension +
      faker==40.37.0 + airflow-init combined repair (data/ chown + passwords-file bind-mount fix)
- [x] 08-02-PLAN.md — Permanent verification (verify-phase8) + fresh-clone/idempotency proof +
      docs/environment.md clean-state rewrite

### Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`)
**Goal**: The full CSV → Oracle pipeline runs unattended once per hour — no manual `make generate`
step, no changes to `csv_ingest.py`/`report_ready.py`.
**Depends on**: Phase 8
**Requirements**: SCHED-01, SCHED-02, SCHED-03, SCHED-04, SCHED-05, SCHED-06, SCHED-07, SCHED-08
**Success Criteria** (what must be TRUE):
  1. The `csv_generate_schedule` DAG is scheduled `@hourly` with `catchup=False` and, once
     unpaused, produces a new automatic DagRun every hour with no manual `make generate` step
     (SCHED-01)
  2. Each hourly run's generated customers+orders CSV pair has a checksum different from the prior
     hour's (per-run-varying seed), producing a new, non-idempotency-no-op `ingestion_metadata` row
     each time (SCHED-02)
  3. Within one hourly run, the three chain-trigger tasks fire strictly in order — customers'
     `csv_ingest` DagRun reaches a terminal success state before orders' begins, and orders
     completes before `report_ready` begins, visible in task/DagRun timestamps (SCHED-03) — and
     `max_active_runs=1` prevents two hourly cycles from running concurrently, visibly queuing a
     second scheduled run if the first is still active (SCHED-04)
  4. If `csv_ingest` or `report_ready` is manually paused, the parent DAG's next run fails
     immediately and visibly (`fail_when_dag_is_paused=True`) rather than hanging (SCHED-05) — and
     `csv_ingest.py`/`report_ready.py` remain byte-for-byte unmodified (`git diff` empty) and are
     still independently triggerable via the Airflow UI/REST API exactly as before (SCHED-06)
  5. Each completed hourly run's parent-DAG log contains one summary line reporting both datasets'
     row counts and report-ready status (SCHED-07) — and an operator can override row
     counts/invalid-ratio for a run via DAG `Param`s at trigger time, with the generated CSVs
     reflecting the overridden values instead of hardcoded defaults (SCHED-08)
**Plans**: TBD
**Implementation note**: the three `TriggerDagRunOperator` chain-trigger tasks use
`deferrable=True` (resolves research/SUMMARY.md's Open Decision in favor of Position B — consistent
with this project's existing "defer, never block a worker slot" convention already applied to
`FileSensor(deferrable=True)` in `csv_ingest` and the custom `OraclePartitionReadyTrigger` in
`report_ready`). This must be live-verified against the exact pinned
`apache-airflow-providers-standard==1.17.0` during planning/execution — research flagged several
still-open upstream Airflow bugs in deferred `TriggerDagRunOperator` mode
(apache/airflow#60049, #57756, #38353, #52247) that are not independently confirmed fixed at this
pinned version. `trigger_run_id` stays unset (auto-generated) in the default/happy path per Out of
Scope; never combine `reset_dag_run=True` with `deferrable=True` and an explicit `trigger_run_id`.
**UI hint**: no

### Phase 10: `OraclePartitionReadyTrigger` Robustness Fix
**Goal**: A transient Oracle connectivity error during `report_ready`'s polling no longer
permanently crashes the deferred sensor, while genuine non-transient errors still surface loudly.
**Depends on**: None (independent of Phases 8-9 — touches a different file,
`airflow/dags/_common/oracle_partition_trigger.py`, with no dependency on the new DAG or
environment changes; sequenced last purely for a clean linear phase numbering, per
research/SUMMARY.md)
**Requirements**: ROBUST-01
**Success Criteria** (what must be TRUE):
  1. A simulated transient `oracledb.OperationalError` during `OraclePartitionReadyTrigger.run()`'s
     polling loop triggers a bounded retry/backoff instead of permanently crashing the deferred
     sensor
  2. After exceeding the bounded retry count/elapsed-time cap, a persisting outage surfaces as a
     visible task failure rather than deferring silently forever
  3. A non-transient error (e.g., a bad query or a dropped/renamed table) still surfaces
     immediately as a visible failure, never silently retried or swallowed
  4. A `connection.close()` failure inside the trigger's `finally` block never masks or replaces
     the original polling exception in the failure ultimately surfaced to Airflow
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 8 → 9 → 10

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Environment & Oracle Foundation | v1.0 | 5/5 | Complete | 2026-08-28 |
| 2. Config Contract & CSV Generator | v1.0 | 5/5 | Complete | 2026-08-29 |
| 3. CSV Processing Engine | v1.0 | 10/10 | Complete | 2026-08-29 |
| 4. Oracle Bulk Load, Idempotency & Engine Entrypoint | v1.0 | 3/3 | Complete | 2026-08-29 |
| 5. Airflow DAG Wiring & Deferrable File Wait | v1.0 | 2/2 | Complete | 2026-08-29 |
| 6. End-to-End Verification, Benchmark, CI & Docs | v1.0 | 5/5 | Complete | 2026-08-30 |
| 7. Correlated Customer-Order Business Report | v1.0 | 6/6 | Complete | 2026-08-30 |
| 8. Environment & Docker Fixes for Container-Side Generation | v1.1 | 2/2 | Complete    | 2026-09-01 |
| 9. Hourly Orchestrator DAG (`csv_generate_schedule`) | v1.1 | 0/TBD | Not started | - |
| 10. `OraclePartitionReadyTrigger` Robustness Fix | v1.1 | 0/TBD | Not started | - |
</content>
