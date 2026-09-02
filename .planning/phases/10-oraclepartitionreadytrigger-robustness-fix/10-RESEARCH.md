# Phase 10: `OraclePartitionReadyTrigger` Robustness Fix - Research

**Researched:** 2026-09-02
**Domain:** Async exception handling / bounded retry-with-backoff inside a custom Airflow `BaseTrigger` (single-file fix)
**Confidence:** HIGH

## Summary

This phase's deep technical questions were already resolved by `.planning/research/PITFALLS.md`
Pitfall 9/10 and locked into 8 auto-decisions in `10-CONTEXT.md` (D-01 through D-08). This research
pass re-verified every one of those findings against the actual current repository state — not the
research snapshot — and confirms nothing has drifted:

- `airflow/dags/_common/oracle_partition_trigger.py`'s `run()` method is **byte-for-byte identical**
  to the code block PITFALLS.md Pitfall 9 quotes (lines 111-125), including the exact bug: `await
  oracledb.connect_async(...)` sits outside the `try` block, and `finally: await connection.close()`
  is unconditional (Pitfall 10's masking risk).
- `oracledb==4.0.2` is still the exact pinned version (`pyproject.toml:11`, `uv.lock`). Its
  exception hierarchy was re-verified live in this session (`uv run python3 -c "import oracledb..."`,
  not just read from PITFALLS.md) and matches exactly: `Error > DatabaseError > {DataError,
  IntegrityError, InternalError, NotSupportedError, OperationalError, ProgrammingError}`, plus a
  sibling `InterfaceError`. No drift.
- `tests/unit/test_oracle_partition_trigger.py`'s existing `AsyncMock`/`patch` scaffolding for
  `oracledb.connect_async` and `asyncio.sleep` is exactly as CONTEXT.md describes it — the new D-08
  tests can copy the existing `_mock_connection()`/`_collect_events()` helpers verbatim and only vary
  the `fetchone`/`connect_async`/`close` side effects.
- No new external packages are introduced by this phase — `oracledb`, `asyncio`, and `logging` are
  already project dependencies / stdlib. The Package Legitimacy Audit gate is therefore not
  triggered.

**Primary recommendation:** Implement D-01 through D-07 as a single rewritten `run()` method (concrete
sketch below) using two new module-level constants (`_MAX_TRANSIENT_RETRIES = 10`,
`_RETRY_BASE_DELAY_SECONDS = 1.0`), extend the existing test file with the 4 D-08 scenarios using the
existing mocking pattern, and gate live fault-injection verification behind an optional manual step
(no `verify-phase10` Makefile target precedent requires it — see Live-Verification Feasibility below)
rather than building new compose-orchestration test infrastructure for a single-file internal fix.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Bounded retry/backoff on transient Oracle connectivity error | Backend/Orchestration (Airflow triggerer, in-process async) | Database (Oracle connection layer is what's failing) | The retry loop lives entirely inside `OraclePartitionReadyTrigger.run()`, an async generator executed by Airflow's triggerer process — there is no browser/API/CDN tier in this phase at all |
| Non-transient error surfacing (bad query, dropped table) | Backend/Orchestration | — | Airflow's own triggerer-level uncaught-exception handling (existing framework behavior, not new code) does the surfacing; this phase only ensures the right exception class reaches it uncaught |
| `finally`-block close-failure isolation | Backend/Orchestration | Database (the connection object itself) | Purely a control-flow correctness fix inside the same async function — no other tier involved |

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ROBUST-01 | `OraclePartitionReadyTrigger.run()` catches `oracledb.OperationalError` specifically with bounded retry/backoff; a genuine non-transient error still surfaces as a visible failure | Confirmed current file matches PITFALLS.md's exact crash-point analysis; confirmed `oracledb==4.0.2`'s exception hierarchy live; concrete `run()` rewrite provided below implementing D-01–D-07; 4 new test cases (D-08) mapped to exact `pytest` node IDs in Validation Architecture section |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Oracle driver:** `python-oracledb`, thin mode, exact-version pinned — already satisfied
  (`oracledb==4.0.2`), this phase changes no dependency versions.
- **Config validation:** N/A to this phase (no `config.json`/Pydantic surface touched).
- **Airflow executor:** LocalExecutor — unaffected; this phase's change runs inside the triggerer
  process, not a worker.
- No other CLAUDE.md directive applies to this phase's narrow scope (single-file exception-handling
  fix, no new DAG/task-graph surface, no new package).

## Standard Stack

No new packages this phase. Existing dependencies used:

| Library | Version (verified) | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `oracledb` | `4.0.2` [VERIFIED: uv.lock, confirmed live via `uv run python3 -c "import oracledb; print(oracledb.__version__)"` → `4.0.2`] | Async Oracle driver; source of the `OperationalError`/`ProgrammingError`/`DatabaseError` hierarchy this fix depends on | Already the project's sole Oracle driver (CLAUDE.md-mandated) |
| `asyncio` (stdlib) | Python 3.12/3.13 stdlib | `asyncio.sleep` for non-blocking backoff inside the triggerer's shared event loop | Only correct choice per the module's own docstring constraint — `time.sleep` would block every other deferred task project-wide |
| `logging` (stdlib) | stdlib | Reuses module's existing `_LOGGER = logging.getLogger("airflow.task")` | D-07 — no new logger instance |

**Installation:** None required — no `pyproject.toml`/`uv.lock` change for this phase.

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Catching `oracledb.OperationalError` narrowly | Catching `oracledb.DatabaseError` broadly | Rejected by D-02/PITFALLS.md — would also catch `ProgrammingError` (genuine bugs), causing infinite silent retry on a broken query |
| Count-based retry cap | Elapsed-time-based cap | Rejected by D-03 — requires wall-clock mocking in tests, more complex to reason about, no material benefit at this project's scale |

## Package Legitimacy Audit

Not applicable — this phase installs zero new external packages. `oracledb==4.0.2` is already
pinned project-wide and unchanged by this phase. No `slopcheck`/registry verification needed.

## Architecture Patterns

### System Architecture Diagram

```
Airflow triggerer process (single shared asyncio event loop, serves every
deferred task project-wide)
        │
        ▼
OraclePartitionReadyTrigger.run()  (async generator, awaited by triggerer)
        │
        ├─► while True:  (poll loop, poke_interval cadence)
        │      │
        │      ├─► try:
        │      │      ├─► await oracledb.connect_async(...)   ◄── moved INSIDE try (D-01)
        │      │      ├─► cursor = connection.cursor()
        │      │      ├─► await cursor.execute(_POLL_QUERY)
        │      │      └─► (count,) = await cursor.fetchone()
        │      │
        │      ├─► except oracledb.OperationalError as exc:    ◄── narrow catch (D-02)
        │      │      ├─► retry_count < _MAX_TRANSIENT_RETRIES?
        │      │      │      ├─► yes: log WARNING, await asyncio.sleep(backoff), continue loop
        │      │      │      └─► no:  re-raise exc                (D-04, exhausted)
        │      │      │                    │
        │      │      │                    ▼
        │      │      │          escapes run() uncaught ──► Airflow triggerer's own
        │      │      │                                     top-level handler fails the
        │      │      │                                     deferred TaskInstance, surfaces
        │      │      │                                     traceback in trigger log
        │      │      │
        │      │      └─► (any other oracledb.Error subclass, e.g. ProgrammingError)
        │      │                    │
        │      │                    ▼
        │      │          NOT caught by this except clause ──► propagates immediately,
        │      │                                                same uncaught-exception path
        │      │                                                as above (D-02, zero extra code)
        │      │
        │      └─► finally:
        │             └─► try: await connection.close()
        │                 except oracledb.Error: log DEBUG, swallow  ◄── D-06, never masks
        │                                                              the original exception
        │
        └─► count >= _BOTH_DATASETS_PRESENT?
               ├─► yes: yield TriggerEvent({"status": "ready"}); return
               └─► no:  await asyncio.sleep(self.poke_interval); loop again
                        (retry_count resets to 0 here — a successful poll clears the
                        transient-failure budget, per Claude's Discretion recommendation)
```

### Recommended Project Structure

No structural change — this phase edits exactly one existing file and extends exactly one existing
test file. No new files.

```
airflow/dags/_common/
└── oracle_partition_trigger.py   # MODIFIED: run() rewritten, 2 new module constants
tests/unit/
└── test_oracle_partition_trigger.py   # MODIFIED: 4 new test cases appended (D-08)
```

### Pattern 1: Narrow-exception bounded retry with exponential backoff (D-01–D-05)

**What:** Catch only the connection/network-level exception subclass; retry a fixed number of
consecutive times with exponential backoff capped at the trigger's own poll cadence; re-raise on
exhaustion using the framework's existing uncaught-exception-fails-the-task mechanism.

**When to use:** Any custom `BaseTrigger.run()` polling an external system where "connection
flaked momentarily" and "the query/logic is broken" must be distinguished and handled differently.

**Example — direct precedent already in this repo** (`scripts/verify_environment.py:241-273`,
`verify_airflow_auth()`):
```python
# Source: scripts/verify_environment.py (this repo, lines 241-272)
for attempt in range(1, AUTH_RETRY_ATTEMPTS + 1):
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode("utf-8"))
        break
    except urllib.error.HTTPError as exc:
        raise AssertionError(...) from exc  # non-transient: fail immediately
    except (urllib.error.URLError, OSError, ...) as exc:  # transient class(es)
        if attempt < AUTH_RETRY_ATTEMPTS:
            delay = min(
                AUTH_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                AUTH_RETRY_MAX_DELAY_SECONDS,
            )
            print(f"... retrying in {delay}s ...", file=sys.stderr)
            time.sleep(delay)  # OK here: synchronous script, not the triggerer's event loop
            continue
        raise AssertionError(f"... failed after {AUTH_RETRY_ATTEMPTS} attempts: {exc}") from exc
```
D-05 explicitly adapts this exact shape (`base_delay * 2**(attempt-1)`, capped) for
`OraclePartitionReadyTrigger`, swapping `time.sleep` for `await asyncio.sleep` (mandatory inside the
triggerer's shared event loop) and swapping the constant names/values per Claude's Discretion.

### Anti-Patterns to Avoid

- **Blanket `except oracledb.Error:` or `except Exception:` around the whole loop body:**
  PITFALLS.md Pitfall 9's explicitly-flagged failure mode — retries a permanently broken query
  forever, `deferred` state with no visible failure, arguably worse than today's crash.
- **`time.sleep` anywhere in `run()`, including inside new except/backoff code:** Stalls the
  triggerer's single shared event loop for every other deferred task project-wide (module's own
  docstring, PITFALLS.md Integration Gotchas table).
- **Unconditional `finally: await connection.close()`:** Pitfall 10 — a close-time failure on an
  already-broken connection replaces (masks) the original, more diagnostic exception.
- **Constructing a custom failure `TriggerEvent` on retry exhaustion:** D-04 rejects this — re-use
  Airflow's existing "uncaught exception in `run()` fails the deferred task" mechanism instead of
  inventing a second failure-signaling shape.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Signaling "this deferred task failed" to Airflow | A custom failure `TriggerEvent` payload + sensor-side branching in `execute_complete()` | Let the exception propagate uncaught out of `run()` | Airflow's triggerer already has this exact mechanism built in (fails the TaskInstance, surfaces the traceback) — D-04 explicitly chooses this over a custom path |
| Distinguishing transient vs. non-transient Oracle errors | A custom regex/string-match on `str(exc)` for ORA-/DPY- codes | `oracledb`'s own exception class hierarchy (`OperationalError` vs. `ProgrammingError`/others) | The driver already classifies these at the class level — verified live, stable across the pinned version — string-matching error messages is fragile and unnecessary |

**Key insight:** Airflow's triggerer and `oracledb`'s exception hierarchy already provide both
mechanisms this phase needs (failure surfacing, transient/non-transient classification) — the entire
fix is wiring the existing `try`/`except`/`finally` structure correctly, not building new
infrastructure.

## Common Pitfalls

### Pitfall 1: `connect_async()` left outside the retry `try` block

**What goes wrong:** A fix that only wraps the pre-existing `try/finally` (cursor/execute/fetch)
in `except OperationalError` still leaves a connection failure — the single most likely transient
case — completely unhandled, because execution never reaches that `try` block.
**Why it happens:** The bug is in the *scope* of the existing `try`, not its exception clause; easy
to miss when only reading the `except` line being added.
**How to avoid:** D-01 — move `connect_async(...)` inside the `try` (or wrap the whole loop body,
connect included, in one `try`). Verified: this is exactly the current file's actual bug (line 113
`connection = await oracledb.connect_async(...)` sits before `try:` on line 116).
**Warning signs:** A test that mocks `connect_async` to raise `OperationalError` and asserts a retry
happens — if it fails with the exception propagating immediately instead of being caught, the scope
is still wrong.

### Pitfall 2: `finally: connection.close()` masking the original exception

**What goes wrong:** An exception raised inside a `finally` block replaces the exception already
propagating from the `try` block — a close-time failure on an already-severed connection (e.g. after
a `DPY-4011` "database or network closed the connection") silently overwrites the real, diagnostic
error.
**Why it happens:** Python's own `finally` semantics — this is language behavior, not an Oracle-
specific quirk.
**How to avoid:** D-06 — nest `try: await connection.close() except oracledb.Error: log at debug,
never re-raise` inside the outer `finally`.
**Warning signs:** A test where `connection.close` is configured to raise, and the assertion checks
that the *original* exception (not the close-failure) is what actually propagates/is captured by
`pytest.raises`.

### Pitfall 3: Backoff computed but never actually bounded by `poke_interval`

**What goes wrong:** An exponential backoff formula (`base * 2**(attempt-1)`) grows unbounded if the
cap is forgotten, meaning a late retry could sleep far longer than the trigger's own normal polling
cadence — surprising operators who expect "roughly every `poke_interval`" behavior even during a
retry storm.
**Why it happens:** Easy to copy `verify_environment.py`'s formula without also copying its `min(...,
MAX_DELAY_SECONDS)` cap.
**How to avoid:** D-05 explicitly caps backoff at `poke_interval` (30.0s default) — `min(base_delay *
2 ** (attempt - 1), self.poke_interval)`.
**Warning signs:** A retry-heavy test run takes far longer than `10 * poke_interval` to complete
without `asyncio.sleep` mocked.

## Code Examples

### Concrete `run()` rewrite (implements D-01–D-07)

This is the literal code the planner should hand to the executor as `<action>` guidance — it
follows the module's existing constant-naming style (`_POLL_QUERY`, `_BOTH_DATASETS_PRESENT`) and
inserts cleanly after those constants, before the `OraclePartitionReadyTrigger` class:

```python
# New module-level constants, placed alongside the existing _POLL_QUERY /
# _BOTH_DATASETS_PRESENT constants (around line 91 of the current file):

# ROBUST-01 (D-03): max consecutive oracledb.OperationalError failures before
# the trigger gives up and re-raises, letting Airflow's triggerer fail the
# deferred task visibly. At the default poke_interval (30.0s) this is a
# ~5-minute transient-outage window before giving up.
_MAX_TRANSIENT_RETRIES = 10

# ROBUST-01 (D-05): exponential backoff base delay between retries, mirroring
# scripts/verify_environment.py's AUTH_RETRY_BASE_DELAY_SECONDS convention.
# Capped at self.poke_interval so backoff never exceeds the trigger's own
# normal polling cadence.
_RETRY_BASE_DELAY_SECONDS = 1.0
```

```python
    async def run(self) -> AsyncIterator[Any]:
        retry_count = 0
        while True:
            try:
                connection = await oracledb.connect_async(
                    user=oracle_user(), password=oracle_password(), dsn=oracle_dsn()
                )
                try:
                    cursor = connection.cursor()
                    await cursor.execute(_POLL_QUERY)
                    (count,) = await cursor.fetchone()
                finally:
                    try:
                        await connection.close()
                    except oracledb.Error:
                        _LOGGER.debug(
                            "connection.close() failed on an already-broken connection",
                            exc_info=True,
                        )
            except oracledb.OperationalError as exc:
                retry_count += 1
                if retry_count > _MAX_TRANSIENT_RETRIES:
                    raise
                delay = min(
                    _RETRY_BASE_DELAY_SECONDS * (2 ** (retry_count - 1)),
                    self.poke_interval,
                )
                _LOGGER.warning(
                    "Oracle poll attempt %d/%d failed (transient): %s -- retrying in %.1fs",
                    retry_count,
                    _MAX_TRANSIENT_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            retry_count = 0  # a successful poll clears the transient-failure budget
            if count >= _BOTH_DATASETS_PRESENT:
                yield TriggerEvent({"status": "ready"})
                return
            await asyncio.sleep(self.poke_interval)
```

**Notes for the planner/executor:**
- `oracledb.ProgrammingError` (and any other `DatabaseError`/`Error` subclass) is *not* named in any
  `except` clause here, so it propagates out of `run()` immediately on first occurrence — exactly
  D-02/success-criterion-3's requirement, with zero additional code.
- `retry_count > _MAX_TRANSIENT_RETRIES` (strictly greater) with `retry_count += 1` happening before
  the check means exactly 10 consecutive `OperationalError`s are tolerated (attempts 1-10 retry,
  the 11th raises) — confirm this exact off-by-one framing when writing the D-08(b) test assertion
  (`OperationalError` on attempts 1-10 retried, 11th call's exception is what's re-raised — i.e. the
  mock's `side_effect` list should have 11 `OperationalError` entries for a clean "exhausted after
  exactly 10 retries" test, or 11 identical `OperationalError()` instances if asserting `is`/`==` on
  which one propagates).
- `await connection.close()` inside the inner guarded `try` still requires `connection` to have been
  successfully assigned — if `oracledb.connect_async(...)` itself is what raised the
  `OperationalError` (the connection was never established), the inner `try/finally` around
  `cursor`/`execute`/`fetchone` never executes, so there is no dangling `connection.close()` call to
  guard for that case. This is correct: nothing to close if nothing connected. The outer `except
  oracledb.OperationalError` catches this cleanly without needing `connection` to be pre-declared.
- `asyncio.sleep` is called in both the retry-backoff path and the existing not-ready poll path —
  both already correctly `await`ed, non-blocking.

### Existing test-mocking pattern to extend (D-08)

```python
# Source: tests/unit/test_oracle_partition_trigger.py (this repo, lines 31-46) —
# reuse _mock_connection()/_collect_events() verbatim; only vary side_effect lists.

def _mock_connection(fetchone_results: list[tuple[int]]) -> MagicMock:
    cursor = MagicMock()
    cursor.execute = AsyncMock(return_value=None)
    cursor.fetchone = AsyncMock(side_effect=fetchone_results)
    connection = MagicMock()
    connection.cursor = MagicMock(return_value=cursor)
    connection.close = AsyncMock(return_value=None)
    return connection

async def _collect_events(trigger: OraclePartitionReadyTrigger) -> list[TriggerEvent]:
    return [event async for event in trigger.run()]
```

For the new D-08 scenarios, `oracledb.connect_async` itself (not just `cursor.fetchone`) needs a
`side_effect` list mixing `OperationalError` instances and successful connection mocks — e.g.:

```python
import oracledb

connect_async_mock = AsyncMock(
    side_effect=[oracledb.OperationalError("DPY-6005: cannot connect"), _mock_connection([(2,)])]
)
with patch(
    "_common.oracle_partition_trigger.oracledb.connect_async", connect_async_mock
), patch("_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)):
    trigger = OraclePartitionReadyTrigger(poke_interval=30.0)
    events = asyncio.run(_collect_events(trigger))
```

For the non-transient-propagates-immediately case (D-08c), `connect_async`'s `side_effect` should be
a single `oracledb.ProgrammingError(...)` and the test should assert via `pytest.raises` around the
`asyncio.run(_collect_events(trigger))` call (not inspect a returned events list, since the
exception escapes before any `TriggerEvent` is produced).

For the close-failure-doesn't-mask case (D-08d), configure `connection.close = AsyncMock(side_effect=
oracledb.Error("close failed"))` on a connection whose `cursor().execute()` (or `connect_async` for
the outer-connect-failure variant) raises `oracledb.OperationalError` first — assert the exception
captured by `pytest.raises` (or the final re-raised exception after exhausting retries) is the
`OperationalError`, not the close-time `oracledb.Error`.

## Runtime State Inventory

Not applicable — this is not a rename/refactor/migration phase. No stored data, service config, OS
registrations, secrets, or build artifacts are affected; this phase edits only in-process Python
control flow inside one already-existing file.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `oracledb.OperationalError`'s `str(exc)` representation never embeds the plaintext connection password (only host/DSN/ORA-/DPY- codes) | Code Examples — warning-level retry log line includes `%s` of the exception | If wrong, the new `warning`-level log line (`_LOGGER.warning(...)`) could leak the Oracle app-schema password into Airflow's trigger logs on every transient retry. Not independently re-verified against `oracledb==4.0.2`'s actual DPY-6005/DPY-4011 message-formatting source in this research pass (no live Oracle-down scenario was triggered to inspect a real exception's `str()` output) — recommend the planner add a quick manual check (trigger one real `OperationalError` locally, e.g. via a wrong port, and eyeball the message) before or during D-08's test-writing, or explicitly note this as a residual risk in the phase's verification notes |
| A2 | No `verify-phase10` Makefile target or live fault-injection harness (`docker compose stop oracle` mid-poll, deliberately-broken `_POLL_QUERY`) is required for this phase to be considered complete — unit-test coverage (D-08's 4 cases) is sufficient given this is an internal robustness fix with no new DAG/task-graph surface | Live-Verification Feasibility section below | If wrong (i.e., the user/planner actually wants live-Oracle fault injection as a phase gate, matching PITFALLS.md's "Looks Done But Isn't" checklist literally), the phase could ship with unit tests green but the actual live triggerer/oracledb interaction unverified — PITFALLS.md itself flags this as the highest-value verification for this exact fix. This is a reasonable default given the project's LOW-risk framing of Pitfall 9 in STATE.md/PITFALLS.md, but is Claude's own judgment call, not a locked CONTEXT.md decision — CONTEXT.md's D-08 only locks the *unit* test shape, leaving live verification unaddressed |

## Open Questions (RESOLVED)

1. **RESOLVED (2026-09-02, planner + plan-checker): unit-test-only — no `10-HUMAN-UAT.md`, no live
   fault-injection phase gate. All 4 ROADMAP success criteria are fully covered by the plan's
   automated unit tests; the manual `docker compose stop oracle` / typo'd-query checks below remain
   optional, undertaken confidence checks, not a blocking obligation.** Should live fault-injection
   verification (docker compose stop oracle mid-poll) be a phase gate or an optional follow-up?
   - What we know: PITFALLS.md's "Looks Done But Isn't" checklist explicitly calls this out as the
     way to verify Pitfall 9/10 are *actually* fixed, not just code-inspected. The `oracle` service
     name is confirmed in `docker-compose.yml` (line 170), so `docker compose stop oracle` /
     `docker compose start oracle` is a valid, ready-to-use fault-injection command.
   - What's unclear: No existing `verify-phaseN` Makefile target in this repo does live fault
     injection (stopping a running service mid-test) — all existing targets (`verify-phase4`
     through `verify-phase9`) run unit/integration suites and static `BundleDagBag` checks against a
     healthy stack, never a deliberately-degraded one. Phase 9 instead used a `09-HUMAN-UAT.md`
     document for exactly this kind of "can't cleanly automate, verify manually and record the
     outcome" scenario (the TriggerRunner deadlock observation).
   - Recommendation: Follow Phase 9's precedent — the planner should decide between (a) a
     `verify-phase10` Makefile target that is just `uv run pytest tests/unit/ -x` (mirrors
     `verify-phase2`/`verify-phase3`'s shape, since this phase touches no live DAG structure), plus
     (b) an optional manual verification note/HUMAN-UAT step describing the `docker compose stop
     oracle` / broken-query live check for a human to run once, rather than building new automated
     compose-orchestration test infrastructure for a single-file fix. This keeps the automated gate
     fast and deterministic while still capturing PITFALLS.md's recommended live check as a
     documented, optional confidence-builder.

## Live-Verification Feasibility

- **`make down`/service-specific stop:** The `Makefile` has `down` (stops the *entire* stack via
  `docker compose down`) but no per-service target. `docker compose stop oracle` (bare compose CLI,
  not a Makefile wrapper) is confirmed valid — `oracle` is the exact service name in
  `docker-compose.yml:170`, and `docker compose start oracle` would bring it back.
  [VERIFIED: docker-compose.yml read directly]
- **No `verify-phase10` target exists yet** — every prior phase from `verify-phase2` through
  `verify-phase9` was added during that phase's own implementation plan (per each phase's own
  `SUMMARY.md`), not pre-existing. This phase should follow the same convention: add
  `verify-phase10` in this phase's plan, not assume the planner needs to invent a new pattern.
  [VERIFIED: `grep` across Makefile + phase SUMMARY.md files]
- **Precedent for "hard to automate, document instead":** Phase 9's `09-HUMAN-UAT.md` is the
  established pattern in this repo for exactly this situation (a real-infrastructure behavior that's
  awkward to assert deterministically in an automated gate). Recommend the planner reuse this
  pattern rather than building new live-fault-injection test tooling from scratch for a phase this
  narrowly scoped.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `connect_async()` outside `try`, unconditional `finally: close()` | `connect_async()` inside `try`, narrow `except OperationalError` with bounded retry, guarded `finally: close()` | This phase (ROBUST-01) | A transient Oracle blip no longer permanently crashes the deferred `report_ready` sensor; a genuine query/schema bug still surfaces immediately, never silently retried forever |

No externally-facing library/ecosystem changes are relevant here — this is purely an internal
control-flow correctness fix using APIs already in use elsewhere in this repo.

## Sources

### Primary (HIGH confidence)
- `airflow/dags/_common/oracle_partition_trigger.py` — read directly in full this session; confirmed
  byte-identical to PITFALLS.md's Pitfall 9 quoted code block.
- `tests/unit/test_oracle_partition_trigger.py` — read directly in full this session.
- `pyproject.toml` / `uv.lock` — grepped directly, confirms `oracledb==4.0.2` pinned exactly.
- Live verification: `uv run python3 -c "import oracledb; print(oracledb.__version__); ..."` run in
  this session — confirmed `4.0.2` and the exact exception MRO
  (`OperationalError -> DatabaseError -> Error -> Exception`, same for `ProgrammingError`).
- `scripts/verify_environment.py` lines 218-273 — read directly, the exact backoff-shape precedent
  D-05 adapts.
- `docker-compose.yml` — read directly, confirms `oracle` service name and healthcheck shape.
- `Makefile` — read directly, confirms no pre-existing live-fault-injection target and the
  established "add `verify-phaseN` during that phase's own plan" convention.
- `.planning/config.json` — read directly, confirms `nyquist_validation: true`,
  `security_enforcement: true`, `security_asvs_level: 1` — both extra RESEARCH.md sections below are
  required, not optional, for this phase.

### Secondary (MEDIUM confidence)
- `.planning/research/PITFALLS.md` Pitfall 9/10 — this phase's primary upstream research; re-verified
  against live code/dependency state in this session rather than taken on faith.

### Tertiary (LOW confidence)
- None — every claim in this research was either read directly from the repository or verified live
  via a command in this session, except A1 in the Assumptions Log (oracledb exception message
  content), which is explicitly flagged as unverified.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages; existing pinned version re-verified live
- Architecture: HIGH — concrete code sketch derived directly from the actual current file plus a
  direct in-repo precedent (`verify_environment.py`)
- Pitfalls: HIGH — PITFALLS.md's Pitfall 9/10 re-verified against live code and live `oracledb`
  import in this session, not just cited from the prior research document

**Research date:** 2026-09-02
**Valid until:** Stable — this is an internal single-file fix with no external API surface;
re-verify only if `oracledb`'s pinned version changes or the trigger file is touched by another
phase first.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `pytest==9.1.1` [VERIFIED: pyproject.toml] |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `pythonpath = ["."]`) |
| Quick run command | `uv run pytest tests/unit/test_oracle_partition_trigger.py -x` |
| Full suite command | `uv run pytest tests/unit/ -x` (matches every existing `verify-phaseN` target's shape) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ROBUST-01 (criterion 1) | Transient `OperationalError` on first poll, success on retry → trigger eventually yields the ready event | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_retries_transient_operational_error_then_succeeds -x` | Extend existing file |
| ROBUST-01 (criterion 2) | `OperationalError` on every attempt through the cap → re-raises after exactly 10 consecutive failures | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_reraises_after_exhausting_transient_retries -x` | Extend existing file |
| ROBUST-01 (criterion 3) | Non-transient error (e.g. `oracledb.ProgrammingError`) on first attempt → propagates immediately, no retry | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_propagates_non_transient_error_immediately -x` | Extend existing file |
| ROBUST-01 (criterion 4) | `connection.close()` itself raising inside `finally` never masks the original exception | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_close_failure_does_not_mask_original_exception -x` | Extend existing file |

Exact test function names above are Claude's Discretion suggestions (D-08 locks the *scenarios*, not
the literal names) — follow the existing file's `test_<verb>_<condition>` naming convention
(`test_run_does_not_yield_when_only_one_dataset_is_present`, etc.) if the planner prefers different
exact names; keep them descriptive enough that a failing test name alone identifies which of the 4
D-08 scenarios broke.

### Sampling Rate

- **Per task commit:** `uv run pytest tests/unit/test_oracle_partition_trigger.py -x`
- **Per wave merge:** `uv run pytest tests/unit/ -x` (full unit suite — matches every prior phase's
  `verify-phaseN` shape)
- **Phase gate:** Full unit suite green before `/gsd:verify-work`; optionally, one manual live check
  (`docker compose stop oracle` mid-poll against a running `report_ready` DagRun, or a deliberately
  typo'd `_POLL_QUERY` — see Live-Verification Feasibility / Open Questions above) recorded in a
  `10-HUMAN-UAT.md` if the planner chooses to include it, mirroring Phase 9's precedent.

### Wave 0 Gaps

None — `tests/unit/test_oracle_partition_trigger.py` already exists with the exact mocking
scaffolding (`AsyncMock`, `_mock_connection()`, `_collect_events()`) the 4 new D-08 tests need; no
new fixture file, conftest, or framework install required.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | This phase touches no authentication flow — Oracle credentials are read via the existing `oracle_user()`/`oracle_password()` env-var helpers, unchanged by this phase |
| V3 Session Management | No | N/A — no session surface in a trigger's polling loop |
| V4 Access Control | No | N/A — no new access-control decision points introduced |
| V5 Input Validation | No | `_POLL_QUERY` is an unchanged, already-parameterless, hardcoded SQL string (no user input reaches this trigger at all) |
| V6 Cryptography | No | No cryptographic operation introduced |
| V7 Error Handling and Logging | **Yes** | This phase's entire purpose is error-handling correctness. Standard control: never log secrets in exception messages/log lines; catch narrowly, not broadly; never silently swallow a non-transient error (see A1 in Assumptions Log for the one open verification item — whether `str(OperationalError)` could ever embed a credential) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Credential leakage via verbose exception logging | Information Disclosure | Log only `str(exc)`/`exc_info=True` at `warning`/`debug` level to Airflow's own task-log stream (not an external sink); confirm (per A1) that `oracledb`'s `OperationalError` message format does not itself embed the plaintext password before shipping the `warning`-level retry log line |
| Denial of service via unbounded retry loop | Denial of Service (self-inflicted — resource/attention exhaustion, not an external attacker) | D-03's bounded retry cap (10 consecutive failures) directly mitigates this — an indefinitely-retrying deferred task with no failure signal was PITFALLS.md's own flagged risk |
| Broad exception catching masking a genuine security-relevant failure (e.g. an authorization error surfaced as a generic `DatabaseError`) | Tampering / Repudiation (masked evidence of a real problem) | D-02's narrow `OperationalError`-only catch directly mitigates this — anything else propagates loudly, preserving the traceback as evidence |

This phase has no user-facing input, no new authentication/authorization surface, and no
cryptographic operation — its security-relevant surface is entirely V7 (error handling/logging
correctness), which is exactly what ROBUST-01 already requires functionally. No new ASVS controls
need to be introduced beyond what D-01–D-07 already specify.
