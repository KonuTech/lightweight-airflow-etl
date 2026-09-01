---
phase: 09-hourly-orchestrator-dag-csv-generate-schedule
plan: 04
subsystem: infra
tags: [airflow, rest-api, live-verification, triggerdagrunoperator, deferrable]

# Dependency graph
requires:
  - phase: 09-hourly-orchestrator-dag-csv-generate-schedule
    provides: "airflow/dags/csv_generate_schedule.py (Plan 09-02) and docs/airflow-dag.md's
      csv_generate_schedule section + Live Verification Evidence placeholder (Plan 09-03) that
      this plan populates with real evidence"
provides:
  - "docs/airflow-dag.md's csv_generate_schedule Live Verification Evidence: real, captured
    REST API JSON proving SCHED-03 (sequential chain-trigger completes end-to-end, deferred
    states observed), SCHED-04 (overlapping cycle queues, never races), and SCHED-05 (paused
    child DAG fails the parent run immediately)"
  - "A live-verified bug fix in csv_generate_schedule.py's generate_task() for Airflow 3.x's
    nullable logical_date on manually/API-triggered runs"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Live REST-API verification of deferred-mode TriggerDagRunOperator behavior against the
      real running stack, following the exact /auth/token + /api/v2/dags/.../dagRuns pattern
      already established for csv_ingest's DAG-03/DAG-05 evidence"

key-files:
  created: []
  modified:
    - airflow/dags/csv_generate_schedule.py
    - docs/airflow-dag.md

key-decisions:
  - "generate_task() falls back to dag_run.run_after when dag_run.logical_date is None -- Airflow
    3.x's logical_date is genuinely nullable for manually/API-triggered runs (confirmed live via
    the SDK's own DagRunProtocol type: logical_date: AwareDatetime | None vs. run_after:
    AwareDatetime, the one field Airflow 3.x guarantees non-null on every DagRun). Discovered only
    by actually triggering with this plan's own documented {\"logical_date\": null} body --
    scheduled runs never hit this path since they always carry a real logical_date."
  - "Deleted four stale, incorrectly-sorted-first leftover CSV fixture files from data/customers/
    and data/orders/ (untracked, .gitignored runtime artifacts from earlier manual testing
    sessions) -- _common/paths.py's resolve_matched_file() deterministically re-globs for the
    sorted-first match by design (Phase 5), and these stale files' names sorted before the
    genuine hourly-generated pair, causing every real @hourly-scheduled run for several hours to
    silently reprocess old data and fail on Phase 7's orders_valid FK trigger."
  - "Force-failed four already-broken, permanently-stuck DagRuns (three real hourly
    csv_generate_schedule runs from 20:00/21:00/22:00 UTC plus their still-running 23:00
    counterpart, and their four corresponding stuck report_ready child runs) via PATCH
    .../dagRuns/{id} {\"state\": \"failed\"} to free the max_active_runs=1 slot immediately,
    rather than waiting up to 45 minutes per run for their dagrun_timeout to fire naturally."

patterns-established: []

requirements-completed: [SCHED-03, SCHED-04, SCHED-05, SCHED-06]

# Metrics
duration: 25min
completed: 2026-09-02
---

# Phase 9 Plan 4: Live Verification of csv_generate_schedule's Chain-Trigger Behaviors Summary

**Live-triggered `csv_generate_schedule` runs against the real docker-compose stack prove SCHED-03/04/05 genuinely work on the pinned Airflow 3.3.1 / `apache-airflow-providers-standard==1.17.0` combination -- closing the phase's own flagged HIGH-confidence risk on `TriggerDagRunOperator(deferrable=True)`, and along the way uncovering (and fixing) two real bugs that were silently failing every actual hourly run for several hours.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-09-01T22:58:00Z
- **Completed:** 2026-09-01T23:23:00Z
- **Tasks:** 3 completed (Task 3 auto-approved per auto-mode checkpoint contract)
- **Files modified:** 2 (plus 4 stale runtime fixture files deleted, untracked/gitignored)

## Accomplishments
- Triggered a genuinely fresh `csv_generate_schedule` run and captured real `deferred` states
  (`trigger.classpath: airflow.providers.standard.triggers.external_task.DagStateTrigger`) for
  `trigger_customers` and `trigger_orders`, then a clean overall `DagRun` `state: "success"` with
  all six tasks firing in strict order -- SCHED-03 live-proven, not just structurally checked
- Explicitly confirmed none of the four flagged upstream issues (#60049/#57756/#38353/#52247)
  reproduced during the live run -- the research's MEDIUM-confidence finding is now
  live-confirmed working on this exact pinned combination
- Triggered two `csv_generate_schedule` runs in immediate succession and captured the second
  genuinely sitting in `queued` state while the first was `running` -- SCHED-04 live-proven
- Paused `csv_ingest`, triggered a new run, and captured `trigger_customers` reaching a terminal
  `failed` state (`exc_type: DagIsPaused`) within ~2 seconds instead of hanging -- SCHED-05
  live-proven, with `csv_ingest` confirmed restored to `is_paused: false` afterward
- Discovered and fixed two real, pre-existing bugs during live triggering (see Deviations): a
  nullable-`logical_date` crash in `generate_task()`, and a stale-fixture-file glob-ordering bug
  that had been silently failing every real hourly `csv_generate_schedule` run for several hours
  before this plan started
- `git diff --stat -- airflow/dags/csv_ingest.py airflow/dags/report_ready.py` confirmed clean
  after all live-triggering activity (SCHED-06)

## Task Commits

Each task was committed atomically:

1. **Task 1: Live scenario A -- full cascade completes, deferred states observed (SCHED-03)** - `5f61336` (feat)
2. **Task 2: Live scenarios B + C -- overlap queuing (SCHED-04) and paused-DAG immediate failure (SCHED-05)** - `d92c8a9` (docs)
3. **Task 3: Review live verification evidence** - auto-approved per auto-mode checkpoint contract (no file changes; review-and-confirm gate only)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified
- `airflow/dags/csv_generate_schedule.py` - `generate_task()` now derives its seed from
  `dag_run.logical_date or dag_run.run_after` instead of `dag_run.logical_date` alone, fixing a
  live-discovered `AttributeError` on manually/API-triggered runs
- `docs/airflow-dag.md` - `csv_generate_schedule`'s "Live Verification Evidence" subsection
  populated with real, captured REST API evidence for SCHED-03/04/05, replacing Plan 09-03's
  placeholder text
- `data/customers/customers_1788283829821223445.csv`,
  `data/customers/customers_1788283847307977164.csv`,
  `data/orders/orders_1788283847307977164.csv`, `data/orders/orders_20260901.csv` (deleted,
  untracked/gitignored runtime artifacts, not tracked by git -- no commit)

## Decisions Made
See `key-decisions` in frontmatter above: (1) `generate_task()`'s null-safe seed derivation, (2)
deletion of stale leftover fixture files that were silently breaking every real hourly run, (3)
force-failing four already-permanently-stuck `DagRun`s to free the `max_active_runs=1` slot
immediately rather than waiting out their 45-minute timeouts.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `generate_task()` crashed with `AttributeError` on a `None` `logical_date`**
- **Found during:** Task 1's first live-trigger attempt (`manual__2026-09-01T23:12:37...`)
- **Issue:** This plan's own documented trigger body, `{"logical_date": null}` (per this file's
  existing "API note" and `scripts/trigger_dag.sh`'s established pattern), produces a DagRun whose
  `dag_run.logical_date` is genuinely `None` in Airflow 3.x -- not auto-assigned to "now" as the
  existing docs implied. `derive_seed(logical_date)` then called `.strftime()` on `None`, crashing
  `generate_task` and cascading to `upstream_failed` for every downstream task.
- **Fix:** Changed `generate_task()` to derive its seed from
  `dag_run.logical_date or dag_run.run_after` -- `run_after` is the one timestamp field Airflow
  3.x's `DagRunProtocol` guarantees non-null on every DagRun. Confirmed via the pinned SDK's own
  type definitions (`airflow/sdk/types.py`) before applying the fix, not assumed.
- **Files modified:** `airflow/dags/csv_generate_schedule.py`
- **Verification:** `uv run pytest tests/unit/dags/ -x` (18 passed), a live `BundleDagBag`
  structural check (`DAGBAG_OK`), and a subsequent successful live-triggered run (the SCHED-03
  evidence captured in this plan).
- **Committed in:** `5f61336` (Task 1 commit)

**2. [Rule 3 - Blocking] Stale leftover CSV fixture files were silently breaking every real hourly run**
- **Found during:** Investigation before Task 1's live trigger, after noticing three real
  `@hourly`-scheduled `csv_generate_schedule` runs (20:00/21:00/22:00 UTC) had already failed on
  their 45-minute `dagrun_timeout`, and a fourth was currently stuck `running`.
- **Issue:** `data/customers/` and `data/orders/` (fully `.gitignore`d, untracked runtime
  directories) contained leftover CSV fixture files from earlier manual testing sessions, with
  names that sorted alphabetically before the genuine hourly-generated `<dataset>_20260901.csv.gz`
  pair. `_common/paths.py`'s `resolve_matched_file()` deterministically re-globs for the
  **sorted-first** match on every `process_csv_task` run (an established Phase 5 design, not a
  bug in that function) -- so every real hourly run was silently reprocessing the same stale data
  instead of the freshly generated pair. For `orders` specifically, the stale file's
  `customer_id`s no longer existed in the continuously-regenerating `customers_valid` table,
  tripping Phase 7's `BEFORE INSERT` FK trigger and returning `DATABASE_ERROR` every time --
  `report_ready`'s sensor then waited forever for an `orders` `ingestion_metadata` row that never
  arrived, and each cascade run failed on its `dagrun_timeout`.
- **Fix:** Deleted the four stale files (`customers_1788283829821223445.csv`,
  `customers_1788283847307977164.csv`, `orders_1788283847307977164.csv`, `orders_20260901.csv`)
  -- confirmed via `git status --short -- data/` that these are untracked runtime artifacts, not
  source, and via a full-repo grep that nothing references them by name. Also force-failed (via
  `PATCH .../dagRuns/{id} {"state": "failed"}`) the four already-permanently-stuck `DagRun`s (the
  three real hourly failures plus the then-`running` 23:00 run, and their four corresponding
  stuck `report_ready` child runs) to free the `max_active_runs=1` slot immediately rather than
  waiting out their remaining ~38-minute `dagrun_timeout` windows.
- **Files modified:** none (runtime data files only, gitignored)
- **Verification:** The subsequent live-triggered run (Task 1's SCHED-03 evidence) genuinely
  processed `customers_20260901.csv.gz`/`orders_20260901.csv.gz` and both datasets reached
  `SUCCESS_WITH_INVALID_ROWS` with real row counts, confirmed via each child DAG's
  `load_results_task` XCom.
- **Committed in:** N/A (no source files changed; documented as an "Environment note" in the
  SCHED-03 evidence block in `docs/airflow-dag.md`, committed in `5f61336`)

---

**Total deviations:** 2 auto-fixed (1 bug, 1 blocking environment issue)
**Impact on plan:** Both fixes were necessary to obtain genuine, non-fabricated live-verification
evidence -- without them, every live-triggered run in this plan would have hit the same silent
failure mode already affecting every real hourly run in the background. No scope creep: neither
fix touched `csv_ingest.py`/`report_ready.py` (SCHED-06 held throughout, confirmed via
`git diff --stat` after each task).

## Issues Encountered
None beyond the two auto-fixed issues documented above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- `docs/airflow-dag.md`'s `csv_generate_schedule` section now carries real, reproducible
  evidence for all three chain-trigger behaviors (SCHED-03/04/05) -- the phase's own flagged
  highest-risk open question is closed.
- `csv_ingest` is confirmed restored to its normal, unpaused, independently-triggerable state.
- `make verify-phase9` passes cleanly against the live stack after this plan's fix
  (231 unit tests + live `BundleDagBag` check, `DAGBAG_OK`).
- Phase 9 is now fully live-verified; ready for `/gsd:verify-work` / phase transition.

---
*Phase: 09-hourly-orchestrator-dag-csv-generate-schedule*
*Completed: 2026-09-02*

## Self-Check: PASSED

- FOUND: airflow/dags/csv_generate_schedule.py
- FOUND: docs/airflow-dag.md
- FOUND: .planning/phases/09-hourly-orchestrator-dag-csv-generate-schedule/09-04-SUMMARY.md
- FOUND: 5f61336 (feat: live-verify csv_generate_schedule cascade reaches success)
- FOUND: d92c8a9 (docs: live-verify overlap queuing and paused-DAG failure)
