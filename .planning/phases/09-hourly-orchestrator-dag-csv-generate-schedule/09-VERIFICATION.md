---
phase: 09-hourly-orchestrator-dag-csv-generate-schedule
verified: 2026-09-02T05:54:48Z
status: human_needed
score: 9/9 must-haves verified (roadmap Success Criteria) + 1 live-system finding requiring human decision
overrides_applied: 0
human_verification:
  - test: "Decide whether the observed Airflow triggerer subprocess deadlock (causing a real, fully-fixed-code scheduled csv_generate_schedule run to fail past its 45-minute dagrun_timeout) is an acceptable residual risk or needs a follow-up hardening task"
    expected: "A documented decision: accept as known Airflow-platform-level risk (matches STATE.md's already-recorded TriggerDagRunOperator deferred-mode blocker category), or open a new requirement/phase for triggerer health monitoring / auto-recovery / alerting"
    why_human: "This is an operational/reliability risk-acceptance judgment call about Airflow's own TriggerRunner subprocess stability under this project's exact environment, not a code defect fixable inside csv_generate_schedule.py itself -- requires a human product/ops decision, not a code fix"
---

# Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`) Verification Report

**Phase Goal:** The full CSV → Oracle pipeline runs unattended once per hour — no manual `make
generate` step, no changes to `csv_ingest.py`/`report_ready.py`.
**Verified:** 2026-09-02T05:54:48Z
**Status:** human_needed
**Re-verification:** No — initial verification (post-code-review-fix)

## Important Context: This Verification Covers Post-Review Code State

09-REVIEW.md found 1 Critical + 3 Warning + 3 Info findings after the four plans (09-01..09-04)
were individually summarized, and all 7 were fixed in 6 additional commits (`bd114b0` through
`a4eeb84`). This verification checks the **current, post-fix** state of the code — confirmed by
reading the live files directly (not SUMMARY.md narrative) and by re-running the phase's own
`make verify-phase9` gate and the full unit suite against the current repo state.

## Goal Achievement

### Observable Truths (ROADMAP.md Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `csv_generate_schedule` scheduled `@hourly`, `catchup=False`, produces a new automatic DagRun every hour once unpaused, no manual `make generate` step (SCHED-01) | VERIFIED | `@dag(dag_id="csv_generate_schedule", schedule="@hourly", catchup=False, max_active_runs=1, ...)` at `airflow/dags/csv_generate_schedule.py:63-73`; `docker-compose.yml:23` sets `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"` (no `is_paused_upon_creation` override in the DAG itself); live `GET /api/v2/dags/csv_generate_schedule` confirms `is_paused: false`; live DagRun list shows automatic `scheduled__*` runs created every hour (20:00 through 05:00 UTC) with zero manual intervention required to create them |
| 2 | Each hourly run's generated CSV pair has a per-run-varying seed/checksum (SCHED-02) | VERIFIED | `derive_seed(logical_date) = int(logical_date.strftime("%Y%m%d%H"))` in `generate_schedule_helpers.py:28-43`; `test_seed_varies_by_hour` and `test_derive_seed_matches_documented_format` pass (`uv run pytest tests/unit/dags/ -q` → 19 passed) |
| 3 | Three chain-trigger tasks fire strictly in order (customers → orders → report_ready), and `max_active_runs=1` visibly queues a second concurrent cycle (SCHED-03/SCHED-04) | VERIFIED | Code: `generate_task() >> trigger_customers >> trigger_orders >> trigger_report_ready` (`csv_generate_schedule.py:231-232`); live evidence in `docs/airflow-dag.md`'s SCHED-03 section shows `trigger_orders` starting only after `trigger_customers` ends (`23:15:51`→`23:15:52`) and `trigger_report_ready` only after `trigger_orders` ends; SCHED-04 section shows a second immediately-triggered run genuinely sitting in `state: "queued"` while the first was `running`; independently re-confirmed live via `make verify-phase9`'s `dag.max_active_runs == 1` assertion (passes, `DAGBAG_OK`) |
| 4 | Paused `csv_ingest`/`report_ready` causes the parent run to fail immediately, not hang (SCHED-05); `csv_ingest.py`/`report_ready.py` remain byte-for-byte unmodified and independently triggerable (SCHED-06) | VERIFIED | `fail_when_dag_is_paused=True` on all 3 `TriggerDagRunOperator` instances (`grep -c` = 3); `docs/airflow-dag.md`'s SCHED-05 section shows `trigger_customers` reaching `state: "failed"` (`exc_type: "DagIsPaused"`) ~2s after a paused-`csv_ingest` trigger, and `csv_ingest` confirmed restored to `is_paused: false`; independently re-confirmed live just now: `git diff --stat -- airflow/dags/csv_ingest.py airflow/dags/report_ready.py` prints nothing, `git log` shows their last touching commits are Phase 5/6/7 (not Phase 9), and live `GET` on all three DAGs currently shows `is_paused: false` |
| 5 | One cascade summary log line per hourly run (row counts + report-ready status), operator-overridable `rows`/`invalid_ratio` Params (SCHED-07/SCHED-08) | VERIFIED | `format_cascade_summary()` unit-tested (`test_summary_format`, `test_summary_format_handles_missing_dataset`); `summary_task` calls `logging.getLogger("airflow.task").info(format_cascade_summary(dataset_results))` after a bind-parameterized `ingestion_metadata` query (`csv_generate_schedule.py:195-213`); `Param(100, type="integer", minimum=1)` / `Param(0.1, type="number", minimum=0.0, maximum=1.0)` at lines 69-72, passed straight through to the `subprocess.run([..., "--rows", str(rows), "--invalid-ratio", str(invalid_ratio), ...])` call; live `make verify-phase9` asserts `dag.params['rows'] == 100` and `dag.params['invalid_ratio'] == 0.1` (passes) |
| 6 | A retention task deletes CSVs older than 30 days, best-effort, never fails the DagRun (SCHED-10) | VERIFIED | `retention_sweep()` fully unit-tested (`test_retention_deletes_old_files`, `test_retention_skips_files_within_window`, `test_retention_does_not_delete_non_canonical_backup_files`, `test_retention_never_raises` — all pass); `retention_task` has `trigger_rule="none_failed_min_one_success"` and no `try/except` needed since `retention_sweep()` itself never raises (confirmed by reading its `try/except ValueError`/`try/except OSError` blocks, `generate_schedule_helpers.py:108-122`) |

**Score:** 6/6 ROADMAP Success Criteria (covering all 9 requirement IDs) VERIFIED against current code.

### Code Review Fix Verification (09-REVIEW.md's 7 findings, all claimed fixed)

| Finding | Claimed Fix | Verified in current code? |
|---|---|---|
| CR-01 (Critical): `skip_when_already_exists` alone masks a failed retry as skip | `reset_dag_run=True` added to all 3 trigger tasks | VERIFIED — `reset_dag_run=True` present on `trigger_customers`, `trigger_orders`, `trigger_report_ready` (`csv_generate_schedule.py:160,174,187`); module docstring and `docs/airflow-dag.md` both updated with the corrected D-07 rationale. **Not independently live-retested** (would require deliberately failing a child DagRun and retrying) — the fix rests on the reviewer's direct trace of the installed `apache-airflow-providers-standard==1.17.0` source, not a live-triggered repro. Reasonable given the effort/risk tradeoff, but flagged for completeness. |
| WR-01/WR-02: subprocess diagnostics discarded, no timeout | `try/except CalledProcessError`/`TimeoutExpired` with full stdout/stderr logging + `timeout=300` | VERIFIED — present at `csv_generate_schedule.py:109-152` |
| WR-03: retention glob over-matches (e.g. `.csv.bak`) | Anchored `_FILENAME_RE = re.compile(r"^(\d{8})\.csv(\.gz)?$")` | VERIFIED — present at `generate_schedule_helpers.py:25`; new test `test_retention_does_not_delete_non_canonical_backup_files` passes |
| IN-01: no deterministic tiebreaker in `_LATEST_INGESTION_SQL` | `ORDER BY processed_at DESC, id DESC` | VERIFIED — present at `csv_generate_schedule.py:55` |
| IN-02: cross-file clock note | Code comment at `derive_seed()`'s call site | VERIFIED — present at `csv_generate_schedule.py:94-104` |
| IN-03: `verify-phase9` only checked one of three trigger tasks | Extended to `all(...)` over all three | VERIFIED — present at `Makefile:134-136`; re-ran live just now: `make verify-phase9` → `DAGBAG_OK` |

All 7 commits (`bd114b0`, `70ab290`, `3f3ad83`, `e25e914`, `ad1c70e`, `6d743bb`, `a4eeb84`) exist
in `git log` and match the Fix Log's description of what each touches.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `airflow/dags/_common/generate_schedule_helpers.py` | `derive_seed()`, `format_cascade_summary()`, `retention_sweep()`, zero Airflow imports | VERIFIED | All three functions present; `grep -c "^from airflow\|^import airflow"` = 0 |
| `tests/unit/dags/test_generate_schedule_helpers.py` | Full unit coverage, exact test names required by 09-VALIDATION.md | VERIFIED | 8 tests present, all pass |
| `airflow/dags/csv_generate_schedule.py` | Complete 6-task DAG, registered at module bottom | VERIFIED | `dag_id="csv_generate_schedule"` present once; `csv_generate_schedule()` invoked at module bottom (line 235) |
| `Makefile`'s `verify-phase9` target | Live `BundleDagBag` structural check | VERIFIED | Present, passes live: `DAGBAG_OK`, all trigger-task flags checked (post-IN-03-fix) |
| `docs/airflow-dag.md` | `csv_generate_schedule` section + Live Verification Evidence (SCHED-03/04/05) | VERIFIED | Section present with real (non-placeholder) captured REST API JSON for all three scenarios |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `csv_generate_schedule.py` | `_common/generate_schedule_helpers.py` | `from _common.generate_schedule_helpers import derive_seed, format_cascade_summary, retention_sweep` | WIRED | Import present line 39-43, all three functions called in task bodies |
| `trigger_customers`/`trigger_orders` | `csv_ingest.py`'s `params={}` contract | `conf={"dataset": ..., "config_path": ...}` | WIRED | Matches `csv_ingest.py`'s Param names exactly; live evidence shows the triggered `csv_ingest` child DagRuns genuinely processing the fresh CSVs |
| `summary_task` | `ingestion_metadata` (Oracle) | `load.get_connection()` + bind-parameterized SELECT | WIRED | `cursor.execute(_LATEST_INGESTION_SQL, dataset=dataset)` — bind param, never f-string interpolated |
| `retention_task` | `_common/paths.py`'s `DATA_ROOT` | `retention_sweep(paths.DATA_ROOT / dataset, dataset, cutoff)` | WIRED | Present at line 225 |

### Behavioral Spot-Checks (run live against the running docker-compose stack, this session)

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full unit suite | `uv run pytest tests/unit/ -q` | 232 passed | PASS |
| `csv_generate_schedule` unit tests | `uv run pytest tests/unit/dags/ -q` | 19 passed | PASS |
| Live structural gate | `make verify-phase9` | `DAGBAG_OK` (232 tests + live `BundleDagBag` assertions) | PASS |
| `csv_ingest.py`/`report_ready.py` unmodified | `git diff --stat -- airflow/dags/csv_ingest.py airflow/dags/report_ready.py` | (empty) | PASS |
| All 3 DAGs currently unpaused | `GET /api/v2/dags/{csv_ingest,csv_generate_schedule,report_ready}` | `is_paused: false` for all three | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|---|---|---|---|---|
| SCHED-01 | 09-02, 09-03 | `@hourly`, `catchup=False`, no manual step | SATISFIED | Code + live DAG-metadata check |
| SCHED-02 | 09-01 | Per-hour varying seed | SATISFIED | Unit tests |
| SCHED-03 | 09-02, 09-04 | Strict sequential chain-trigger | SATISFIED | Code + live REST evidence |
| SCHED-04 | 09-02, 09-03, 09-04 | `max_active_runs=1`, queues not races | SATISFIED | Code + live REST evidence |
| SCHED-05 | 09-02, 09-03, 09-04 | `fail_when_dag_is_paused=True` | SATISFIED | Code + live REST evidence |
| SCHED-06 | 09-02, 09-04 | `csv_ingest.py`/`report_ready.py` unmodified | SATISFIED | `git diff` empty, `git log` confirms |
| SCHED-07 | 09-01, 09-02 | One cascade summary log line | SATISFIED | Code + unit tests (no live task-log capture of the actual logged line, but wiring is trivial/direct) |
| SCHED-08 | 09-02, 09-03 | Operator-overridable `rows`/`invalid_ratio` Params | SATISFIED | Code + live `verify-phase9` Param-default assertion (no live override-value run captured, but the Param→subprocess pass-through is a direct, unambiguous one-line wiring) |
| SCHED-10 | 09-01, 09-02 | Best-effort 30-day retention | SATISFIED | Unit tests (never-raises contract fully covered) |

No orphaned requirements — all 9 IDs (SCHED-01–08, SCHED-10) declared across the four plans'
`requirements:` frontmatter are accounted for. SCHED-09 is explicitly out of scope for this phase
per REQUIREMENTS.md (deferred — would require modifying `csv_ingest.py`, which SCHED-06 forbids).

### Anti-Patterns Found

None. `grep -n "TBD\|FIXME\|XXX\|TODO\|HACK\|PLACEHOLDER"` across
`airflow/dags/csv_generate_schedule.py`, `airflow/dags/_common/generate_schedule_helpers.py`, and
`Makefile` returns zero matches.

## Independent Live-System Finding (discovered during this verification, not in any SUMMARY/REVIEW)

While verifying SCHED-01's "produces a new automatic DagRun every hour" claim against the
currently-running docker-compose stack (containers up ~13h, started before this phase's own live
verification work), I queried the real recent `scheduled__*` DagRun history rather than trusting
the docs' captured evidence alone:

```
2026-09-02T05:00:00Z  running   (in progress at time of check)
2026-09-02T00:00:00Z  failed
2026-09-01T23:00:00Z  failed
2026-09-01T22:00:00Z  failed
2026-09-01T21:00:00Z  failed
2026-09-01T20:00:00Z  failed
```

The `20:00`/`21:00`/`22:00` failures and part of the `23:00` failure are already explained and
documented by 09-04-SUMMARY.md's own deviations section (stale leftover CSV fixture files +
a nullable-`logical_date` bug, both fixed, plus those specific stuck runs were manually
force-failed by the plan itself to clear the `max_active_runs=1` queue).

The `2026-09-02T00:00:00Z` run is **not** explained by any documented deviation — it was created
automatically after all 7 review fixes were committed, with no manual interference. Tracing it:

- `generate_task`, `trigger_customers` both succeeded quickly.
- The underlying triggered `csv_ingest` child DagRun for `orders` genuinely **succeeded in 6
  seconds** (`queued_at 02:05:35` → `end_date 02:05:42`).
- But the parent `trigger_orders` task's `DagStateTrigger` didn't receive/report that completion
  event until `05:46:02` — **over 3.5 hours later** — well past the DAG's
  `dagrun_timeout=timedelta(minutes=45)`, so the parent DagRun was killed as `failed` at
  `05:46:00.997` (a race won by the timeout, one second before the stale trigger event finally
  fired).
- `docker logs lightweight-airflow-etl-airflow-triggerer-1` confirms the mechanism:
  ```
  2026-09-02T02:05:15Z [error] TriggerRunner subprocess event loop appears deadlocked:
    no communication received for 7611.4s (threshold: 30.0s). Skipping heartbeat...
  2026-09-02T05:46:00Z [error] TriggerRunner subprocess event loop appears deadlocked:
    no communication received for 13217.2s (threshold: 30.0s). Skipping heartbeat...
  ```

This is a real, currently-reproducing instance of Airflow's own `TriggerRunner` subprocess
deadlocking for hours at a time — the same general risk category STATE.md/09-RESEARCH flagged
(open upstream `TriggerDagRunOperator`/deferred-mode issues #60049/#57756/#38353/#52247) and which
09-04's live verification explicitly checked for and reported as "not reproduced." That check was
run in a short (~10 minute) manual-triggering window right after the fixes landed — it did not,
and could not, catch a deadlock that only manifested hours later during real unattended scheduled
operation.

As of this verification (`05:54:48Z`), the triggerer has self-recovered (Airflow's own watchdog
detected the deadlock and reassigned triggers; `/api/v2/monitor/health` now reports
`triggerer: healthy`), and the currently-running `05:00:00` scheduled run's three trigger tasks
are progressing normally (`success`, `success`, `deferred`). So this is not a permanently broken
system — but it did cause one genuine, unattended, fully-fixed-code hourly run to fail outright,
with no automatic retry (`retries=0` by design, D-09), requiring a human to notice and manually
re-trigger or investigate — which is in tension with the phase goal's "runs unattended... no
manual step" framing.

This is very plausibly attributable to this specific sandboxed dev environment's resource
constraints (this session's own heavy Docker/pytest/live-trigger activity, on top of the
09-04 plan's own extensive live-triggering) rather than a defect in this phase's own DAG code —
`csv_generate_schedule.py`/`generate_schedule_helpers.py` contain no code that touches Airflow's
`TriggerRunner` internals. It is not something this phase's own deliverables can fix.

## Human Verification Required

### 1. Accept or escalate the triggerer-subprocess-deadlock risk

**Test:** Review the "Independent Live-System Finding" section above.
**Expected:** A documented decision — either (a) accept this as a known residual risk in the same
category already recorded in STATE.md's Blockers/Concerns section (an Airflow-platform-level
`TriggerRunner`/deferred-mode stability issue, self-recovering, not a defect in this phase's code),
or (b) open a follow-up requirement/phase for triggerer health monitoring, alerting on stuck
`DagRun`s, or a bounded auto-retry policy for chain-trigger tasks specifically to reduce the
"needs manual attention" window when this reproduces during real unattended operation.
**Why human:** This is a risk-acceptance/scoping judgment call about Airflow's own internal
subprocess stability under this project's specific environment — not something resolvable by
further code changes to this phase's own files, and not something a grep/structural check can
adjudicate.

## Gaps Summary

No must-have from ROADMAP.md's Success Criteria or the four plans' `must_haves` frontmatter is
FAILED. All 9 requirement IDs are implemented, unit-tested, and — for the three behaviors that
specifically needed it (SCHED-03/04/05) — live-verified with real captured REST API evidence, all
re-confirmed against the current post-code-review-fix code in this session (`make verify-phase9`
→ `DAGBAG_OK`, 232 unit tests passed, `git diff` clean for `csv_ingest.py`/`report_ready.py`).

The one open item is not a gap in what was built, but a live-observed operational risk discovered
by independently querying the running stack's real scheduled-run history rather than relying on
the phase's own (narrower-window) live-verification evidence: an Airflow triggerer subprocess
deadlock caused one genuine automatic hourly run to fail past its timeout, in the same risk
category this phase already flagged and partially investigated. This needs a human decision, not
a code fix, hence `status: human_needed` rather than `gaps_found`.

---

_Verified: 2026-09-02T05:54:48Z_
_Verifier: Claude (gsd-verifier)_
