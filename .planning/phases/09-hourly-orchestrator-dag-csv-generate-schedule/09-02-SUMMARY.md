---
phase: 09-hourly-orchestrator-dag-csv-generate-schedule
plan: 02
subsystem: infra
tags: [airflow, taskflow, trigger-dagrun, oracle, python]

# Dependency graph
requires:
  - phase: 09-hourly-orchestrator-dag-csv-generate-schedule
    provides: "derive_seed(), format_cascade_summary(), retention_sweep() from
      Plan 09-01's generate_schedule_helpers.py"
provides:
  - "airflow/dags/csv_generate_schedule.py -- the complete hourly orchestrator
    DAG: generate_task -> trigger_customers -> trigger_orders ->
    trigger_report_ready -> summary_task -> retention_task"
affects: [09-03-structural-verification, 09-04-live-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "TriggerDagRunOperator chain-trigger cascade: deferrable=True,
      wait_for_completion=True, skip_when_already_exists=True,
      fail_when_dag_is_paused=True, deterministic Jinja trigger_run_id
      ('{{ dag_run.run_id }}__<suffix>'), wired strictly sequentially"
    - "Bind-parameterized direct Oracle query from a worker task
      (load.get_connection() + try/finally connection.close()), mirroring
      report_ready.py's build_report_task shape"

key-files:
  created:
    - airflow/dags/csv_generate_schedule.py
  modified: []

key-decisions:
  - "Split the plan's two tasks into two atomic commits exactly as the plan's
    own Task 1/Task 2 boundary specifies, even though both tasks touch the
    same single file -- Task 1 commit contains only the @dag header,
    generate_task, and the three chain-trigger tasks; Task 2 commit adds
    summary_task, retention_task, final wiring, and the module-bottom
    csv_generate_schedule() registration call."
  - "Verified mypy via the project's own canonical whole-repo invocation
    (uv run mypy ., matching Makefile's lint target) in addition to the
    plan's literal single-file command, after discovering the single-file
    invocation reports a spurious 'missing py.typed marker' error for
    csv_processor that is pre-existing and reproduces identically on the
    already-committed, unmodified report_ready.py -- not specific to this
    plan's new file."

patterns-established: []

requirements-completed: [SCHED-01, SCHED-03, SCHED-04, SCHED-05, SCHED-06, SCHED-08, SCHED-10]

# Metrics
duration: 8min
completed: 2026-09-01
---

# Phase 9 Plan 2: csv_generate_schedule Orchestrator DAG Summary

**Complete six-task hourly orchestrator DAG (`generate_task` -> three sequential `TriggerDagRunOperator` chain-triggers -> `summary_task` -> `retention_task`) that regenerates CSVs and cascades through the existing, byte-for-byte-unmodified `csv_ingest`/`report_ready` DAGs.**

## Performance

- **Duration:** 8 min
- **Started:** 2026-09-01T19:37:02Z (session start; file work began ~19:40)
- **Completed:** 2026-09-01T19:45:13Z
- **Tasks:** 2 completed
- **Files modified:** 1

## Accomplishments
- `@dag(dag_id="csv_generate_schedule", schedule="@hourly", catchup=False, max_active_runs=1, dagrun_timeout=timedelta(minutes=45))` with `rows`/`invalid_ratio` JSON-Schema-validated Params (SCHED-01/SCHED-04/SCHED-08)
- `generate_task` derives its seed from the DagRun's own `logical_date` via `derive_seed()` and invokes `generator/generate_csv.py --correlated` as a subprocess, with `retries=0` (SCHED-02)
- Three `TriggerDagRunOperator` tasks (`trigger_customers`, `trigger_orders`, `trigger_report_ready`), each `deferrable=True`, `wait_for_completion=True`, `skip_when_already_exists=True`, `fail_when_dag_is_paused=True`, `poke_interval=10`, wired strictly sequentially (SCHED-03/SCHED-05) — `trigger_orders` never starts before `trigger_customers` fully commits, honoring Phase 7's `orders_valid` FK-existence DB trigger
- `summary_task` queries `ingestion_metadata` per dataset via bind-parameterized SQL (`cursor.execute(_LATEST_INGESTION_SQL, dataset=dataset)`) and logs one cascade summary line via `format_cascade_summary()` (SCHED-07)
- `retention_task` calls `retention_sweep()` per dataset against a fixed 30-day cutoff, logging deletions/skips, never failing the DagRun (SCHED-10)
- `csv_ingest.py`/`report_ready.py` remain byte-for-byte unmodified — confirmed via empty `git diff --stat` (SCHED-06)

## Task Commits

Each task was committed atomically:

1. **Task 1: DAG header, generate_task, and sequential chain-trigger tasks** - `67851b0` (feat)
2. **Task 2: summary_task, retention_task, final wiring, and SCHED-06 self-check** - `861df6f` (feat)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified
- `airflow/dags/csv_generate_schedule.py` - the complete hourly orchestrator DAG; six tasks (`generate_task`, `trigger_customers`, `trigger_orders`, `trigger_report_ready`, `summary_task`, `retention_task`), wired strictly sequentially, module-bottom `csv_generate_schedule()` invocation registers the DAG

## Decisions Made
- Split plan execution into two commits matching the plan's own Task 1/Task 2 boundary (see `key-decisions` in frontmatter above for full rationale).
- Verified mypy via both the plan's literal single-file command and the project's canonical whole-repo `uv run mypy .` command, after finding the single-file invocation produces a spurious pre-existing error unrelated to this plan's changes (see Deviations below).

## Deviations from Plan

### Auto-fixed Issues

None — no code-level auto-fixes were needed; the plan's exact parameter values and structure were implementable as written against the verified interfaces from `csv_ingest.py`, `report_ready.py`, `load.py`, and Plan 09-01's helpers.

### Notes (not deviations, verification-process findings)

**1. Single-file `uv run mypy <path>` invocation reports a spurious `csv_processor` "missing py.typed marker" error**
- **Found during:** Task 1 verification (`uv run mypy airflow/dags/csv_generate_schedule.py`)
- **Investigation:** Reproduced the identical error running the same single-file command against the already-committed, unmodified `airflow/dags/report_ready.py` — confirms this is a pre-existing quirk of invoking mypy on a single target file (loses `explicit_package_bases`/`mypy_path` resolution context needed to see `csv_processor`'s local editable install as typed), not something introduced by this plan's new file.
- **Resolution:** Verified instead via the project's own canonical whole-repo command (`uv run mypy .`, matching `Makefile`'s `lint` target and CI's `ci.yml` step) after clearing a stale `.mypy_cache` (a separate, unrelated caching artifact that briefly produced a different transient error on the first run). Whole-repo mypy passes clean for `csv_generate_schedule.py`.
- **No files modified** — verification-process finding only, not a code change.

**2. Pre-existing mypy error in `tests/unit/dags/test_generate_schedule_helpers.py:44` (Plan 09-01's file, untouched by this plan)**
- **Found during:** Task 2 verification (whole-repo `uv run mypy .`)
- **Issue:** `format_cascade_summary`'s test passes a `dict[str, dict[str, int]]` literal against a `dict[str, dict[str, int] | None]`-typed parameter; mypy's invariant `dict` typing flags this `[arg-type]`.
- **Scope decision:** Out of scope per the executor's SCOPE BOUNDARY rule — this file was committed in Plan 09-01 (`b965756`), not modified by Plan 09-02. Logged to `.planning/phases/09-hourly-orchestrator-dag-csv-generate-schedule/deferred-items.md` rather than fixed here.

---

**Total deviations:** 0 auto-fixed code changes; 2 verification-process notes documented (1 pre-existing out-of-scope mypy issue deferred, 1 single-file-vs-whole-repo mypy invocation quirk explained).
**Impact on plan:** None — plan executed exactly as written; both notes above are process/verification findings, not scope creep.

## Issues Encountered
None beyond the verification-process notes documented above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- `airflow/dags/csv_generate_schedule.py` is complete, registered, and passes `ruff check` and whole-repo `mypy` clean.
- `csv_ingest.py`/`report_ready.py` confirmed byte-for-byte unmodified (SCHED-06) via `git diff --stat`.
- Plan 09-03 (structural verification, e.g. `make verify-phase9`) and Plan 09-04 (live-triggered verification) can now proceed — both depend on this file existing and being well-formed.
- One pre-existing, out-of-scope mypy issue in Plan 09-01's test file is logged in `deferred-items.md` for a future cleanup pass.

---
*Phase: 09-hourly-orchestrator-dag-csv-generate-schedule*
*Completed: 2026-09-01*

## Self-Check: PASSED

- FOUND: airflow/dags/csv_generate_schedule.py
- FOUND: .planning/phases/09-hourly-orchestrator-dag-csv-generate-schedule/09-02-SUMMARY.md
- FOUND: .planning/phases/09-hourly-orchestrator-dag-csv-generate-schedule/deferred-items.md
- FOUND: 67851b0 (feat: DAG header, generate_task, chain triggers)
- FOUND: 861df6f (feat: summary_task, retention_task, final wiring)
