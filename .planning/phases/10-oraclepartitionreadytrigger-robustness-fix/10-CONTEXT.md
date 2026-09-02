# Phase 10: `OraclePartitionReadyTrigger` Robustness Fix - Context

**Gathered:** 2026-09-02 (via `--auto`, no user interaction)
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix `OraclePartitionReadyTrigger.run()` (`airflow/dags/_common/oracle_partition_trigger.py`) so a
transient Oracle connectivity error during its polling loop triggers a bounded retry/backoff
instead of permanently crashing the deferred sensor, while a genuine non-transient error (bad
query, dropped/renamed table) still surfaces immediately and loudly — never silently retried or
swallowed. Does NOT touch `csv_generate_schedule.py`, `csv_ingest.py`, or `report_ready.py`'s own
task graph — this phase is scoped entirely to the trigger's internal exception handling.

</domain>

<decisions>
## Implementation Decisions

### Exception taxonomy (ROBUST-01 core fix)

- **D-01 [auto]:** `oracledb.connect_async(...)` moves inside a `try` block covering the whole poll
  cycle (connect + cursor + execute + fetch), not just the cursor/execute/fetch portion — Pitfall 9
  identified the current code leaves `connect_async()` outside `try` entirely, so a connection
  failure (the single most likely transient-error case) is completely unhandled today.
  [auto] Area: "Exception scope" — Q: "Should connect_async() be inside or outside the retry try
  block?" → Selected: "Inside, covering the full poll cycle" (recommended; matches Pitfall 9's
  explicit finding).
- **D-02 [auto]:** Catch `oracledb.OperationalError` specifically for the retry-with-backoff path —
  never the broader `oracledb.DatabaseError` or bare `oracledb.Error`. `OperationalError` is the
  connection/network-level branch of the verified exception hierarchy (DPY-6005, DPY-4011,
  ORA-12541/03113/01033-class TNS/listener/instance issues). Everything else (`ProgrammingError`,
  `IntegrityError`, etc.) propagates uncaught — Airflow's triggerer already fails the deferred task
  and surfaces the traceback for any exception escaping `run()`, so this needs zero custom
  "fail loudly" code, only *not* catching those exceptions.
  [auto] Area: "Exception specificity" — Q: "Catch broad DatabaseError or narrow
  OperationalError?" → Selected: "Narrow OperationalError only" (recommended; matches
  PITFALLS.md's own Technical Debt Patterns table: "narrow-and-correct is the right tradeoff").

### Bounded retry policy

- **D-03 [auto]:** Retry cap is **consecutive-failure-count-based**, not elapsed-time-based —
  simpler to reason about and test deterministically (no wall-clock mocking needed). Max
  **10 consecutive** `OperationalError` failures before giving up.
  [auto] Area: "Retry cap shape" — Q: "Count-based or elapsed-time-based bound?" → Selected:
  "Count-based, max 10 consecutive failures" (recommended; simpler to test, and at the existing
  `poke_interval=30.0` default this is ~5 minutes of retrying before giving up — a reasonable
  transient-outage window without retrying forever).
- **D-04 [auto]:** On exceeding the retry cap, **re-raise the last caught `OperationalError`** (do
  not construct a custom failure `TriggerEvent`) — reuses Airflow's existing "uncaught exception in
  `run()` fails the deferred task, traceback surfaced in the trigger's log" mechanism, the same
  zero-custom-code path D-02 already relies on for non-retryable errors. Simpler and more
  consistent than introducing a second failure-signaling shape.
  [auto] Area: "Exhausted-retry signal" — Q: "Re-raise last exception, or yield a custom failure
  TriggerEvent?" → Selected: "Re-raise last exception" (recommended; consistent with D-02's
  already-established "let Airflow's own uncaught-exception handling do the work" pattern).
- **D-05 [auto]:** Backoff between retries uses `asyncio.sleep` with exponential backoff (never a
  blocking `time.sleep` — PITFALLS.md's Integration Gotchas table: any blocking call here stalls
  the triggerer's single shared event loop for every other deferred task project-wide). Base delay
  and cap mirror `scripts/verify_environment.py`'s existing `AUTH_RETRY_ATTEMPTS`/
  `AUTH_RETRY_BASE_DELAY_SECONDS` exponential-backoff convention (`base_delay * 2**(attempt-1)`),
  adapted to this trigger: base delay 1.0s, capped at `poke_interval` (30.0s) so backoff never
  exceeds the trigger's own normal polling cadence.
  [auto] Area: "Backoff strategy" — Q: "Fixed poke_interval sleep or exponential backoff between
  retries?" → Selected: "Exponential backoff capped at poke_interval, matching
  verify_environment.py's existing pattern" (recommended; reuses an established in-repo
  convention rather than inventing a new one).

### `finally` block close-failure masking (Pitfall 10)

- **D-06 [auto]:** Guard `connection.close()` inside its own nested `try`/`except oracledb.Error`,
  logging at `debug` level (`"connection.close() failed on an already-broken connection"`,
  `exc_info=True`) and never re-raising — so a close-time failure on an already-severed connection
  never masks/replaces the original, more diagnostic exception propagating from the `try` block.
  Exact pattern from PITFALLS.md's Pitfall 10 fix.
  [auto] Area: "finally-block close-failure handling" — Q: "Guard connection.close() or leave it
  unconditional?" → Selected: "Guard with nested try/except, log at debug, never re-raise"
  (recommended; matches Pitfall 10's exact documented fix, same code block as D-01–D-05).

### Logging conventions

- **D-07 [auto]:** Use the module's existing `_LOGGER = logging.getLogger("airflow.task")` for all
  new log lines — matches the module's own established logger, no new logger instance. Log each
  retried `OperationalError` at `warning` level with attempt count
  (`"Oracle poll attempt %d/%d failed (transient): %s — retrying in %.1fs"`); log the final
  exhausted-retry re-raise implicitly via the propagating exception (no extra log line needed,
  Airflow's own trigger-failure handling already surfaces it).
  [auto] Area: "Logging level for retries" — Q: "warning or info for each retried transient
  failure?" → Selected: "warning" (recommended; a retry is worth an operator's attention even
  though it's not yet a failure — matches this project's general "don't silently mask, but don't
  cry wolf on eventual success" instinct).

### Test coverage

- **D-08 [auto]:** Extend `tests/unit/test_oracle_partition_trigger.py` using its existing
  `unittest.mock.patch`/`AsyncMock` conventions for `oracledb.connect_async` and `asyncio.sleep`
  (already used for the happy-path test). New test cases: (a) a transient `OperationalError` on
  the first poll followed by success on the second — trigger eventually yields the ready event;
  (b) `OperationalError` on every attempt up to the cap — trigger re-raises after exactly 10
  consecutive failures; (c) a non-transient error (e.g. `oracledb.ProgrammingError`) on the first
  attempt — propagates immediately, no retry attempted; (d) `connection.close()` itself raising
  inside `finally` on a connection that already failed — the *original* exception (not the close
  failure) is what ultimately propagates/is asserted on.
  [auto] Area: "Test coverage shape" — Q: "New test file or extend existing?" → Selected: "Extend
  tests/unit/test_oracle_partition_trigger.py" (recommended; this project's established one-file-
  per-module test convention, existing file already has the exact mocking scaffolding needed).

### Claude's Discretion

- Exact retry-count constant name/placement (e.g. `_MAX_TRANSIENT_RETRIES = 10` as a module-level
  constant alongside the existing `_POLL_QUERY`/`_BOTH_DATASETS_PRESENT` constants) — implementation
  detail, follow the module's existing constant-naming convention.
- Exact backoff-delay constant names (`_RETRY_BASE_DELAY_SECONDS` or similar) — same as above.
- Whether the retry-attempt counter resets to 0 after any single successful poll cycle (recommended:
  yes — a transient blip followed by full recovery should not count against a later, unrelated
  transient blip's own budget) or persists across the whole `run()` lifetime — planner's call, but
  reset-on-success is the more intuitive "10 consecutive failures" semantic per D-03's own wording.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (this phase's exact scope)
- `.planning/ROADMAP.md` §Phase 10 — goal, depends-on (None — independent of Phases 8-9), and the
  4 literal success criteria (bounded retry, exhausted-retry visible failure, non-transient errors
  still surface immediately, `finally`-block close failure never masks the original exception).
- `.planning/REQUIREMENTS.md` — ROBUST-01 full text: "catches `oracledb.OperationalError`
  specifically with bounded retry/backoff; a genuine non-transient error... still surfaces as a
  visible failure."

### Research (already resolved the deep technical questions — not open)
- `.planning/research/PITFALLS.md` — Pitfall 9 (exact crash-point analysis, verified
  `oracledb==4.0.2` exception hierarchy live in a throwaway venv, the "catch OperationalError only"
  fix) and Pitfall 10 (the `finally: connection.close()` masking problem and its exact fix) — both
  drive every decision in this CONTEXT.md's `<decisions>` section directly. Also: Technical Debt
  Patterns table (narrow-OperationalError tradeoff), Integration Gotchas table (never block the
  triggerer's shared event loop), Performance Traps table (unbounded retry-forever is not
  meaningfully better than today's crash), and the "Looks Done But Isn't" checklist's
  `OraclePartitionReadyTrigger` entry (verify via a real `docker compose stop oracle` mid-poll test
  AND a deliberately-broken `_POLL_QUERY` test, not just code inspection).

### Existing code this phase modifies (more authoritative than research sketches)
- `airflow/dags/_common/oracle_partition_trigger.py` — the sole file this phase modifies.
  `OraclePartitionReadyTrigger.run()` (lines 111-125) is the exact code block; `_LOGGER`,
  `_POLL_QUERY`, `_BOTH_DATASETS_PRESENT` module-level constants already exist and should be
  followed for style. `ReportReadySensor` (lines 128-139) is unaffected — this phase changes only
  the trigger's internal polling/retry behavior, not the sensor's `defer()`/`execute_complete()`
  contract.
- `scripts/verify_environment.py` — `AUTH_RETRY_ATTEMPTS`/`AUTH_RETRY_BASE_DELAY_SECONDS` and
  `verify_airflow_auth()`'s exponential-backoff shape (lines 48-49, 218-271) — the direct style
  precedent D-05 adapts for this trigger's own backoff.
- `tests/unit/test_oracle_partition_trigger.py` — existing test file with the
  `unittest.mock.patch`/`AsyncMock` scaffolding for `oracledb.connect_async` and `asyncio.sleep`
  that D-08's new tests extend.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/verify_environment.py`'s `AUTH_RETRY_ATTEMPTS`/`AUTH_RETRY_BASE_DELAY_SECONDS` constants
  and `verify_airflow_auth()`'s exponential-backoff loop — direct style/shape template for D-05.
- `tests/unit/test_oracle_partition_trigger.py`'s existing `AsyncMock`-based
  `oracledb.connect_async`/`asyncio.sleep` patching pattern (already used for the happy-path test)
  — direct template for D-08's new failure-scenario tests.

### Established Patterns
- "Never block the triggerer's single shared event loop" — this module's own docstring already
  states this (`oracledb.connect_async()`, never blocking `connect()`); D-05 extends the same
  discipline to backoff sleeps (`asyncio.sleep`, never `time.sleep`).
- "Fail loudly, don't silently mask" — this project's general instinct (seen in Phase 9's
  `fail_when_dag_is_paused=True`, `retries=0` choices) directly drives D-02/D-04's "let genuine
  bugs propagate uncaught" design.
- This module's `try`/`except ModuleNotFoundError` fallback shim (for testing without `apache-
  airflow` installed) is unaffected by this phase — no changes needed there.

### Integration Points
- None beyond the single file. `ReportReadySensor.execute()`/`execute_complete()` and
  `report_ready.py`'s task graph are untouched — this phase is a pure internal-robustness change to
  `OraclePartitionReadyTrigger.run()`.

</code_context>

<specifics>
## Specific Ideas

None — this phase ran entirely in `--auto` mode with no user interaction. All decisions above are
Claude's recommended defaults, chosen to match PITFALLS.md's already-detailed, live-verified
findings and this project's existing conventions (`verify_environment.py`'s retry/backoff shape,
this module's existing constant/logger style). No user-specific preferences to capture.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within Phase 10's domain (the trigger's internal exception handling).

### Reviewed Todos (not folded)
None — `todo.match-phase 10` returned zero matches (no `.planning/todos/pending/` entries).

</deferred>

---

*Phase: 10-`OraclePartitionReadyTrigger` Robustness Fix*
*Context gathered: 2026-09-02 (--auto)*
