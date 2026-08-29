---
phase: 05-airflow-dag-wiring-deferrable-file-wait
plan: 02
subsystem: infra
tags: [airflow, rest-api, deferrable-operator, csv-processor, encoding-detection, docker-compose]

# Dependency graph
requires:
  - phase: 05-airflow-dag-wiring-deferrable-file-wait
    provides: "airflow/dags/csv_ingest.py (Plan 05-01) -- the config-driven DAG this plan proves
      end-to-end for a second dataset without modifying it"
provides:
  - "scripts/trigger_dag.sh -- a reusable REST-API trigger wrapper reusing scripts/
    verify_environment.py's exact /auth/token -> Bearer auth flow"
  - "Makefile's verify-phase5 target -- unit suite + live BundleDagBag structure check"
  - "docs/airflow-dag.md -- task graph, triggering instructions, and reproducible live-verification
    evidence for DAG-03 (deferred state) and DAG-05 (orders dataset)"
  - "A real bug fix in packages/csv-processor/src/csv_processor/source.py closing WINDOWS.md entry
    #1 -- prepare_source() no longer raises an uncaught LookupError when encoding detection
    legitimately returns source='undetermined'"
affects: [06-e2e-benchmark-ci-docs]

actuals:
  tokens: 4600
  tasks: 2
  commits: 3

tech-stack:
  added: []
  patterns:
    - "trigger_dag.sh passes an explicit \"logical_date\": null in the POST dagRuns body --
      this environment's Airflow 3.3.1 REST API marks logical_date as a required (but nullable)
      TriggerDAGRunPostBody field; omitting it returns HTTP 422"
    - "GET .../wait requires an explicit &interval=<seconds> query param in this environment --
      no default value in the openapi.json schema, unlike the plan's literal curl example"
    - "verify-phase5's DagBag structure check uses BundleDagBag (matching 05-01-SUMMARY.md's own
      recorded deviation), not the plan's literal airflow.models.DagBag, since plain DagBag never
      adds the dags folder to sys.path and csv_ingest.py's own _common import fails under it"

key-files:
  created:
    - scripts/trigger_dag.sh
    - docs/airflow-dag.md
    - tests/unit/test_source_undetermined_encoding.py
  modified:
    - Makefile
    - packages/csv-processor/src/csv_processor/source.py

key-decisions:
  - "verify-phase5's DagBag check reproduces 05-01's own proven working BundleDagBag invocation,
    not the 05-02-PLAN.md text's literal airflow.models.DagBag snippet -- 05-01-SUMMARY.md already
    documented that the literal snippet fails with ModuleNotFoundError under plain DagBag."
  - "A brand-new Airflow metadata database starts every DAG paused by default; a paused DAG accepts
    a triggered DagRun but the scheduler never executes its tasks (the run sits in queued
    indefinitely). Documented as a one-time PATCH /dags/csv_ingest {\"is_paused\": false} setup
    step in docs/airflow-dag.md, distinct from any DAG/infra code change."
  - "jq (required by trigger_dag.sh and this plan's own live-verification curl commands) was
    installed as a static binary to ~/.local/bin from the official jqlang GitHub releases page --
    the host environment had no jq and no passwordless apt/sudo access; this is a well-known,
    non-project system CLI tool, not a project package-manager dependency, so the Rule 3
    package-install exclusion (slopsquatting risk) does not apply."

requirements-completed: [DAG-03, DAG-05, DAG-04]

coverage:
  - id: D1
    description: "scripts/trigger_dag.sh triggers csv_ingest via the exact /auth/token -> Bearer
      flow already proven in scripts/verify_environment.py, printing only the dag_run_id to
      stdout"
    requirement: DAG-05
    verification:
      - kind: e2e
        ref: "live invocation for both customers and orders datasets this session; both DagRuns
          reached state == success"
        status: pass
    human_judgment: false
  - id: D2
    description: "wait_for_file's task instance reports Airflow state deferred via the REST API
      taskInstances endpoint when the target file is genuinely absent at trigger time"
    requirement: DAG-03
    verification:
      - kind: e2e
        ref: "GET /api/v2/dags/csv_ingest/dagRuns/{run_id}/taskInstances/wait_for_file ->
          state == \"deferred\", observed twice this session (once before, once after the
          source.py encoding fix) for the orders dataset before its fixture file existed"
        status: pass
    human_judgment: false
  - id: D3
    description: "The identical, unmodified csv_ingest.py (unchanged since Plan 05-01) completes
      the orders dataset with state == success and a SUCCESS/SUCCESS_WITH_INVALID_ROWS status,
      proving DAG-05's config-driven generality with genuine live evidence"
    requirement: DAG-05
    verification:
      - kind: e2e
        ref: "GET /api/v2/dags/csv_ingest/dagRuns/{run_id}/wait?result=load_results_task&interval=1
          -> state == \"success\", results.load_results_task.dataset == \"orders\",
          status == \"SUCCESS_WITH_INVALID_ROWS\" (verified this session)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Makefile's verify-phase5 target runs the full unit suite plus a live DagBag
      structure check in one self-contained command"
    requirement: DAG-04
    verification:
      - kind: integration
        ref: "make verify-phase5 (211 unit tests pass; DAGBAG_OK printed from the live
          airflow-scheduler container, verified this session)"
        status: pass
    human_judgment: false
  - id: D5
    description: "docs/airflow-dag.md documents the task graph, triggering instructions, and both
      live-verification proofs with reproducible curl commands"
    requirement: DAG-03
    verification:
      - kind: other
        ref: "docs/airflow-dag.md contains \"deferred\" and \"orders\" sections with the actual
          observed JSON responses from this session"
        status: pass
    human_judgment: false

duration: 55min
completed: 2026-08-29
status: complete
---

# Phase 5 Plan 2: Airflow DAG Wiring & Deferrable File-Wait -- Live Verification Summary

**A reusable `scripts/trigger_dag.sh` + `make verify-phase5` gate, plus genuine live REST-API evidence that `wait_for_file` reports Airflow state `deferred` and that the unmodified `csv_ingest.py` completes a second dataset (`orders`) end-to-end -- surfacing and fixing a real, previously-flagged `LookupError` bug in `csv_processor`'s encoding-detection cross-check along the way.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-08-29T19:52:03Z
- **Completed:** 2026-08-29T20:12:40Z
- **Tasks:** 2
- **Files modified:** 5 (2 created new: `scripts/trigger_dag.sh`, `docs/airflow-dag.md`, `tests/unit/test_source_undetermined_encoding.py`; 2 modified: `Makefile`, `packages/csv-processor/src/csv_processor/source.py`)

## Accomplishments

- Built `scripts/trigger_dag.sh`, a reusable REST-API trigger wrapper reusing
  `scripts/verify_environment.py`'s exact `/auth/token` -> `Bearer` auth flow, and a
  `verify-phase5` Makefile target combining the full unit suite with a live `BundleDagBag`
  structure check (matching 05-01's own proven working verification method, not the plan text's
  literal `airflow.models.DagBag` snippet, which 05-01-SUMMARY.md already documented as broken).
- Proved DAG-03 live: triggered the `orders` dataset before any fixture file existed and observed
  `wait_for_file`'s task instance reporting Airflow state `deferred` via the REST API
  `taskInstances` endpoint -- twice, once before and once after this session's bug fix.
- Proved DAG-05 live: the same, completely unmodified `csv_ingest.py` from Plan 05-01 completed
  the `orders` dataset end-to-end (`state == "success"`, `status == "SUCCESS_WITH_INVALID_ROWS"`),
  with zero dataset-specific code changes required.
- Found and fixed a real, previously-flagged bug (WINDOWS.md ledger entry #1, from Phase 3 Plan
  08) live during the `orders` trigger: `source.py`'s `prepare_source()` called
  `codecs.lookup(enc_detection.encoding)` unconditionally, one line before checking
  `enc_detection.source` -- an `"undetermined"` detection result (a documented sentinel, never a
  real codec name) raised an uncaught `LookupError` instead of silently deferring to the dataset's
  configured encoding, exactly as D-28 and the function's own inline comment already required.
  Added a regression test, RED-verified against the pre-fix code, then reverted to confirm the fix
  actually closes the gap. Ledger entry #1 marked `fixed`.
- Wrote `docs/airflow-dag.md`: the task graph, `scripts/trigger_dag.sh` usage, and the exact
  reproducible curl commands + observed JSON responses for both live proofs, plus two Airflow
  3.3.1 REST API notes this environment surfaced (`logical_date` required-but-nullable on trigger,
  `interval` required-with-no-default on `wait`).

## Task Commits

1. **Task 1: trigger_dag.sh + Makefile verify-phase5 target** - `20ba5c2` (feat)
2. **Bug fix (found during Task 2's live verification): undetermined-encoding LookupError +
   trigger_dag.sh logical_date fix** - `3050845` (fix)
3. **Task 2: Live proof of DAG-03/DAG-05 + docs** - `97975bf` (docs)

**Plan metadata:** (this commit)

## Files Created/Modified

- `scripts/trigger_dag.sh` - REST-API trigger wrapper, reuses `verify_environment.py`'s auth flow,
  prints only `dag_run_id` to stdout
- `Makefile` - `verify-phase5` target (unit suite + live `BundleDagBag` structure check)
- `docs/airflow-dag.md` - Task graph, triggering instructions, live-verification evidence
- `packages/csv-processor/src/csv_processor/source.py` - Fixed `prepare_source()`'s unconditional
  `codecs.lookup()` call on a possibly-`"undetermined"` encoding-detection sentinel
- `tests/unit/test_source_undetermined_encoding.py` - Regression test proving the fix

## Decisions Made

- `verify-phase5`'s DagBag structure check uses `BundleDagBag` (matching 05-01-SUMMARY.md's own
  recorded working method), not the plan text's literal `airflow.models.DagBag` -- the latter
  never adds the dags folder to `sys.path`, so `csv_ingest.py`'s own `from _common import ...`
  fails under it.
- Documented a one-time "unpause the DAG" setup step in `docs/airflow-dag.md` for any brand-new
  Airflow metadata database (e.g. after `make reset`) -- new DAGs are paused by default, and a
  paused DAG's manually-triggered run sits in `queued` forever since the scheduler skips paused
  DAGs entirely. This is an operational note, not a code/infra change.
- Installed `jq` as a static binary to `~/.local/bin` (official jqlang GitHub release) since the
  host had neither `jq` nor passwordless `sudo`/`apt` access -- `jq` is a well-known system CLI
  tool required verbatim by this plan's own action text, not a project package-manager dependency,
  so the Rule 3 slopsquatting-risk exclusion for `npm install`/`pip install`-style commands does
  not apply.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `source.py`'s `prepare_source()` raised uncaught `LookupError` on an
"undetermined" encoding detection**
- **Found during:** Task 2, live-triggering the `orders` dataset for DAG-05's proof
- **Issue:** `detect_encoding()` documents that `.encoding` is the literal string `"undetermined"`
  (never a real codec name) whenever `source == "undetermined"`. `prepare_source()` called
  `codecs.lookup(enc_detection.encoding).name` unconditionally, one line before checking
  `enc_detection.source`, so an undetermined result raised `LookupError: unknown encoding:
  undetermined` -- caught by `engine.process()`'s generic `except Exception:`, surfacing as
  `Status.PROCESSING_ERROR` with 0 rows processed instead of the correct
  `SUCCESS_WITH_INVALID_ROWS`. This is the exact bug WINDOWS.md ledger entry #1 (Phase 3 Plan 08)
  had already flagged as an open, content-dependent edge case -- now reproduced with real
  generator-produced `orders` bytes, not just a synthetic corpus fixture.
- **Fix:** Moved the `codecs.lookup()` call inside the `if enc_detection.source == "detected":`
  guard so `"undetermined"` correctly falls through to the existing "defer to config" branch
  (D-28), matching the function's own pre-existing inline comment.
- **Files modified:** `packages/csv-processor/src/csv_processor/source.py`,
  `tests/unit/test_source_undetermined_encoding.py` (new regression test)
- **Verification:** Regression test RED-verified against the pre-fix code (reproduced the exact
  `LookupError`, then reverted), GREEN after the fix; full unit suite (211 tests) passes; live
  re-trigger of the `orders` dataset against a rebuilt Airflow image reached
  `state == "success"` with `status == "SUCCESS_WITH_INVALID_ROWS"`.
- **Committed in:** `3050845`
- **Ledger:** WINDOWS.md entry #1 marked `fixed` via `gsd-tools windows fixed 1`.

**2. [Rule 3 - Blocking] `trigger_dag.sh`'s trigger request missing a required `logical_date`
field**
- **Found during:** Task 2, first live trigger attempt
- **Issue:** This environment's Airflow 3.3.1 REST API (`openapi.json`, confirmed by direct
  inspection) marks `logical_date` as a required (but nullable) `TriggerDAGRunPostBody` field --
  the plan's own literal curl examples (both 05-01-PLAN.md's and 05-02-PLAN.md's) omit it, which
  returns HTTP 422 (`Field required`) rather than triggering a run.
- **Fix:** `trigger_dag.sh` now passes `"logical_date": null` explicitly in the POST body.
- **Files modified:** `scripts/trigger_dag.sh`
- **Verification:** Live trigger for both `customers` and `orders` succeeded after the fix.
- **Committed in:** `3050845`

**3. [Rule 3 - Blocking] `GET .../wait` requires an explicit `interval` query parameter**
- **Found during:** Task 2, polling the `customers` run for completion
- **Issue:** This environment's `openapi.json` shows `interval` (seconds between state polls) as a
  required parameter on `GET .../dagRuns/{id}/wait` with no default value -- omitting it returns
  HTTP 422 (`Field required`).
- **Fix:** Added `&interval=1` to every `wait` invocation used for this plan's own live
  verification; documented in `docs/airflow-dag.md` as an environment-specific API note. Not
  applied to `trigger_dag.sh` itself, which never calls `/wait`.
- **Files modified:** none (verification-command only; documented in `docs/airflow-dag.md`)
- **Committed in:** `97975bf` (docs commit)

**4. [Rule 3 - Blocking] Fresh worktree environment lacked a `simple_auth_manager_passwords.json.
generated` pre-seed, `data/` ownership, and `jq`**
- **Found during:** Task 2, standing up this worktree's own docker-compose stack (the main
  checkout's leftover stack from Plan 05-01 occupied the same host ports, so it was stopped and
  this worktree's own stack brought up instead, matching this plan's own precondition text)
- **Issue:** (a) The gitignored `docker/airflow/simple_auth_manager_passwords.json.generated`
  didn't exist in this fresh worktree; Docker auto-created it as a directory on first mount
  (`IsADirectoryError`), and an empty file caused `simple_auth_manager` to auto-generate a random
  password instead of the documented `admin`/`admin` (README.md/docs/environment.md's own
  documented first-clone step: `echo '{"admin": "admin"}' > docker/airflow/
  simple_auth_manager_passwords.json.generated`). (b) `./data/` was bind-mount-created
  `root:root`-owned (same known gap 05-01-SUMMARY.md already documented). (c) `jq` was not
  installed on the host and passwordless `sudo`/`apt` was unavailable.
- **Fix:** (a) Pre-seeded the file per the already-documented first-clone step, `chmod 666`,
  full `docker compose down && up` cycle (Docker Desktop/WSL2 caches bind-mount inode references,
  requiring a full recreate rather than an in-place file swap). (b) `docker run --rm -v
  "$(pwd)/data:/data" alpine:3.20 chown -R 1000:1000 /data`. (c) Installed a static `jq` binary
  to `~/.local/bin` from the official jqlang GitHub releases.
- **Files modified:** none (local filesystem/environment state only, not repo content --
  `docker/airflow/simple_auth_manager_passwords.json.generated` and `data/` are both gitignored)
- **Committed in:** n/a (not a repo change)

---

**Total deviations:** 4 auto-fixed (1 Rule 1 - real bug with production impact, 3 Rule 3 -
blocking environment/API gaps)
**Impact on plan:** All four were necessary preconditions for this plan's own live-verification
requirement to be met at all. The Rule 1 fix (source.py's `LookupError`) is the most significant
-- it closes a previously-known, previously-deferred defect (WINDOWS.md entry #1) that would have
otherwise silently blocked any orders-dataset run whose generated bytes happen to produce an
`"undetermined"` encoding-detection outcome, not just this plan's own live trigger. No scope creep
into Phase 6 territory (automated E2E test, benchmark, CI, docs remain untouched).

## Issues Encountered

- This worktree's own docker-compose stack (project name `agent-a3b7b51316179378e`, derived from
  the worktree directory) could not run alongside the main checkout's already-running stack
  (project name `lightweight-airflow-etl`, apparently left running from Plan 05-01's own session)
  -- both bind the same host ports (`127.0.0.1:8080`, `127.0.0.1:1521`). Resolved by stopping the
  main checkout's stack (`docker compose -p lightweight-airflow-etl down`, no `-v`, so its Oracle/
  Postgres volumes are untouched) and bringing up this worktree's own stack instead, matching the
  plan's own precondition ("The docker-compose stack from Plan 05-01 must already be running"),
  fulfilled from this worktree's checkout rather than the main one. No parallel worktree agents
  were active at the time (`git worktree list` confirmed only this worktree + the main checkout).

## User Setup Required

None - no external service configuration required beyond this session's own one-time environment
bring-up (documented above), which any fresh `make up` will need to repeat per
`docs/environment.md`'s already-existing first-clone guidance.

## Next Phase Readiness

- Both `customers` (Plan 05-01) and `orders` (this plan) datasets are now proven end-to-end
  against the real running stack, using the identical `csv_ingest.py` -- Phase 6's own automated
  E2E test (TEST-03) can build directly on `scripts/trigger_dag.sh` and the documented auth/
  trigger/wait REST flow.
- The `csv_processor` encoding-detection bug (WINDOWS.md entry #1) is now closed with a
  regression test -- Phase 6's E2E/benchmark work no longer risks hitting this specific
  `LookupError` on real generated data.
- Two environment-specific Airflow 3.3.1 REST API quirks (`logical_date` required-but-nullable on
  trigger, `interval` required-with-no-default on `wait`) are documented in `docs/airflow-dag.md`
  for Phase 6's own automated test to account for.
- This worktree's docker-compose stack was left running (not torn down) so the plan's own
  `verify-phase5`/live-trigger evidence remains reproducible for inspection; a fresh `make up`
  from the merged main checkout will need the same one-time `simple_auth_manager_passwords.json.
  generated` pre-seed + `data/` chown steps documented above and in `docs/environment.md`.

## Known Stubs

None.

## Threat Flags

None -- `source.py`'s fix only corrects existing cross-check logic to match its own documented
contract (no new surface); `trigger_dag.sh`'s `logical_date: null` addition and `docs/
airflow-dag.md`'s documentation add no new input surface beyond what Plan 05-01's threat model
(T-05-01 through T-05-05) already covers.

## Self-Check: PASSED

All 3 created files (`scripts/trigger_dag.sh`, `docs/airflow-dag.md`,
`tests/unit/test_source_undetermined_encoding.py`) confirmed present on disk; all 3 task commit
hashes (`20ba5c2`, `3050845`, `97975bf`) confirmed present in `git log`; `make verify-phase5`
passes (211 unit tests + live `DAGBAG_OK`); WINDOWS.md ledger entry #1 confirmed `fixed`.

---
*Phase: 05-airflow-dag-wiring-deferrable-file-wait*
*Completed: 2026-08-29*
