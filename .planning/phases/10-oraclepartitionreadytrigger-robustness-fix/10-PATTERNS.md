# Phase 10: `OraclePartitionReadyTrigger` Robustness Fix - Pattern Map

**Mapped:** 2026-09-02
**Files analyzed:** 2 (both modified, zero new files)
**Analogs found:** 2 / 2

This is a single-file robustness fix plus its test file. There is no controller/component/service
layering to classify — both files already exist, both analogs already exist in this same repo, and
RESEARCH.md's own "Concrete `run()` rewrite" section is itself the primary pattern source (it was
derived directly from reading the current file + the analog below, not invented). This PATTERNS.md
restates those excerpts in the planner-facing format and adds line-numbered provenance.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|--------------------|------|-----------|-----------------|----------------|
| `airflow/dags/_common/oracle_partition_trigger.py` (`OraclePartitionReadyTrigger.run()`, lines 111-125) | trigger / poller (async generator, closest role bucket: **service** — a long-lived polling loop, not a request-response controller) | streaming (async generator) + event-driven (bounded retry/backoff on a transient failure) | `scripts/verify_environment.py` (`verify_airflow_auth()`, lines 218-273) | role-match (both are "poll an external system, retry narrowly-classified transient failures with exponential backoff, let non-transient failures raise immediately") — data-flow differs only in sync `time.sleep`/`for` loop vs. async `await asyncio.sleep`/`while True` shape |
| `tests/unit/test_oracle_partition_trigger.py` (extend with 4 new test cases) | test | request-response (unit test invoking an async generator to completion or exception) | same file, existing tests (lines 57-96) | exact — extending, not analog-borrowing; existing `_mock_connection()`/`_collect_events()` helpers (lines 31-45) are reused verbatim by the new tests |

No other files are created or modified this phase (`ReportReadySensor`, `report_ready.py`,
`csv_generate_schedule.py`, `csv_ingest.py` are explicitly out of scope per CONTEXT.md's `<domain>`).

## Pattern Assignments

### `airflow/dags/_common/oracle_partition_trigger.py` (trigger/service, streaming + event-driven)

**Analog:** `scripts/verify_environment.py` — `verify_airflow_auth()`

**Current buggy state to fix** (`airflow/dags/_common/oracle_partition_trigger.py` lines 111-125):
```python
    async def run(self) -> AsyncIterator[Any]:
        while True:
            connection = await oracledb.connect_async(
                user=oracle_user(), password=oracle_password(), dsn=oracle_dsn()
            )
            try:
                cursor = connection.cursor()
                await cursor.execute(_POLL_QUERY)
                (count,) = await cursor.fetchone()
            finally:
                await connection.close()
            if count >= _BOTH_DATASETS_PRESENT:
                yield TriggerEvent({"status": "ready"})
                return
            await asyncio.sleep(self.poke_interval)
```
Two bugs this phase fixes: (1) `connect_async()` sits outside `try` — a connection failure is
completely unhandled; (2) `finally: await connection.close()` is unconditional — a close-time
failure on an already-broken connection masks the original exception (Pitfall 10).

**Analog retry/backoff pattern** (`scripts/verify_environment.py` lines 241-272,
`verify_airflow_auth()`'s `for attempt in range(1, AUTH_RETRY_ATTEMPTS + 1):` loop):
```python
    for attempt in range(1, AUTH_RETRY_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = json.loads(response.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as exc:
            raise AssertionError(
                f"Airflow auth request failed with HTTP {exc.code}: {exc.read().decode('utf-8')}"
            ) from exc
        except (
            urllib.error.URLError,
            OSError,
            http.client.IncompleteRead,
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as exc:
            if attempt < AUTH_RETRY_ATTEMPTS:
                delay = min(
                    AUTH_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                    AUTH_RETRY_MAX_DELAY_SECONDS,
                )
                print(
                    f"Airflow auth attempt {attempt}/{AUTH_RETRY_ATTEMPTS} failed "
                    f"({exc}) -- retrying in {delay}s (likely a transient cold-start "
                    "race, see G-01-1)",
                    file=sys.stderr,
                )
                time.sleep(delay)
                continue
            raise AssertionError(
                f"Airflow auth request failed after {AUTH_RETRY_ATTEMPTS} attempts: {exc}"
            ) from exc
```
**Constants precedent** (`scripts/verify_environment.py` lines 48-50):
```python
AUTH_RETRY_ATTEMPTS = 6
AUTH_RETRY_BASE_DELAY_SECONDS = 1.0
AUTH_RETRY_MAX_DELAY_SECONDS = 8.0
```

**Divergence from the analog (deliberate, per CONTEXT.md decisions):**
- Analog: sync `time.sleep`, `raise AssertionError(...) from exc` on both non-transient and
  exhausted-retry paths (it's a standalone verification script, not a framework-hosted trigger).
- This phase: `await asyncio.sleep(...)` — mandatory, sync `time.sleep` would stall the triggerer's
  single shared event loop for every other deferred task project-wide (D-05; module's own
  docstring, lines 12-16). Non-transient errors and exhausted-retry both **re-raise the original
  exception directly** (D-04) rather than wrapping in `AssertionError` — Airflow's own triggerer
  uncaught-exception handling is the framework equivalent of the analog's manual
  `raise AssertionError(...) from exc`, so no custom wrapper type is needed here.
- Analog's cap is a *separate* `AUTH_RETRY_MAX_DELAY_SECONDS` constant; this phase caps backoff at
  `self.poke_interval` (an existing instance attribute) instead of a third module constant — a
  small deliberate simplification, not a missed pattern, since the trigger's own natural polling
  cadence is already the right ceiling (D-05).

**Fixed target pattern to implement** (verified against current file's exact structure; matches
RESEARCH.md's "Concrete `run()` rewrite" section verbatim — copy this, not a re-derivation):
```python
# New module-level constants, placed alongside the existing _POLL_QUERY /
# _BOTH_DATASETS_PRESENT constants (currently lines 83-91):
_MAX_TRANSIENT_RETRIES = 10
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

**Logging pattern** (existing module convention, line 76 — no new logger instance):
```python
_LOGGER = logging.getLogger("airflow.task")
```
Use `_LOGGER.warning(...)` for each retried transient failure (D-07), `_LOGGER.debug(..., exc_info=True)`
for the guarded `close()` failure (D-06) — matches the module's existing single-logger convention;
do not instantiate a second logger.

**Error handling pattern:** narrow `except oracledb.OperationalError` only (D-02) — everything else
(`oracledb.ProgrammingError`, any other `DatabaseError`/`Error` subclass) is not named in any
`except` clause in this file, so it propagates immediately and uncaught out of `run()`, reaching
Airflow's own triggerer-level uncaught-exception handling with zero additional code. This mirrors
the analog's `except urllib.error.HTTPError` (immediate, non-retried) vs.
`except (urllib.error.URLError, OSError, ...)` (retried) split, just with the "immediate" branch
implemented as *absence of an except clause* rather than an explicit `raise AssertionError` — because
this is a framework-hosted trigger with its own uncaught-exception-fails-the-task mechanism, not a
standalone script needing a hand-rolled failure signal.

---

### `tests/unit/test_oracle_partition_trigger.py` (test, request-response over an async generator)

**Analog:** same file, existing tests (self-referential — this is an extension, not a new-pattern
borrow)

**Existing scaffolding to reuse verbatim** (lines 31-45):
```python
def _mock_connection(fetchone_results: list[tuple[int]]) -> MagicMock:
    """A mock ``AsyncConnection`` whose ``cursor().fetchone()`` yields each
    value in ``fetchone_results`` in turn, one per call."""
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

**Existing happy-path test pattern to mirror the shape of** (lines 57-75, `patch` on
`connect_async`/`asyncio.sleep`):
```python
def test_run_does_not_yield_when_only_one_dataset_is_present() -> None:
    connection = _mock_connection([(1,), (2,)])

    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(return_value=connection),
        ),
        patch(
            "_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)
        ) as mock_sleep,
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=30.0)
        events = asyncio.run(_collect_events(trigger))

    assert len(events) == 1
    mock_sleep.assert_awaited_once_with(30.0)
```

**New imports needed** for the 4 D-08 test cases (not currently imported in this file — add
alongside the existing `from unittest.mock import AsyncMock, MagicMock, patch` block, lines 20 and
25-28):
```python
import oracledb
import pytest
```
(`pytest` is needed for `pytest.raises` around the two propagating-exception scenarios; `oracledb`
is needed to reference `oracledb.OperationalError`/`oracledb.ProgrammingError`/`oracledb.Error`
directly, matching RESEARCH.md's Code Examples section.)

**New test case shapes (D-08 a-d), each varying only `connect_async`'s/`connection.close`'s
`side_effect`, per RESEARCH.md's "Code Examples" section:**

1. **(a) transient retry then success** — `connect_async` side_effect list:
   `[oracledb.OperationalError("..."), _mock_connection([(2,)])]`; assert exactly one
   `TriggerEvent` with `{"status": "ready"}` is yielded, and `asyncio.sleep` was awaited once for
   the backoff delay (in addition to any poll-interval sleep, per whether both-datasets-present is
   hit on the second attempt).
2. **(b) exhausted retries re-raises** — `connect_async` side_effect: 11 identical
   `oracledb.OperationalError(...)` instances (10 tolerated retries + the 11th that triggers
   `retry_count > _MAX_TRANSIENT_RETRIES`); wrap `asyncio.run(_collect_events(trigger))` in
   `pytest.raises(oracledb.OperationalError)`.
3. **(c) non-transient propagates immediately** — `connect_async` side_effect: a single
   `oracledb.ProgrammingError(...)`; wrap in `pytest.raises(oracledb.ProgrammingError)`; assert
   `asyncio.sleep` was never awaited (no retry attempted).
4. **(d) close-failure doesn't mask original exception** — use `_mock_connection(...)` but override
   `connection.close = AsyncMock(side_effect=oracledb.Error("close failed"))` on a connection whose
   `cursor().execute()` raises `oracledb.OperationalError` first (or use a `connect_async` failure
   variant, per RESEARCH.md's note that there's nothing to close if `connect_async` itself never
   succeeded — prefer the `cursor().execute()`-raises variant here so the guarded `finally: close()`
   path is actually exercised); assert via `pytest.raises`/the final re-raised exception that the
   captured exception is the `OperationalError`, never the close-time `oracledb.Error`.

Follow the existing file's `test_<verb>_<condition>` naming convention (e.g.
`test_run_does_not_yield_when_only_one_dataset_is_present`) for the 4 new test names — exact names
are Claude's Discretion per CONTEXT.md; RESEARCH.md's Validation Architecture section suggests:
- `test_run_retries_transient_operational_error_then_succeeds`
- `test_run_reraises_after_exhausting_transient_retries`
- `test_run_propagates_non_transient_error_immediately`
- `test_run_close_failure_does_not_mask_original_exception`

## Shared Patterns

### Async, never blocking, inside the triggerer's shared event loop
**Source:** `airflow/dags/_common/oracle_partition_trigger.py` module docstring (lines 12-16) —
already-established project rule, not new this phase.
**Apply to:** All new code in `run()` — `await asyncio.sleep(...)` for every backoff/poll delay,
never `time.sleep`. This is the one place the chosen analog (`verify_environment.py`) must be
adapted rather than copied literally (sync script vs. async trigger).

### Narrow exception catching over broad
**Source:** `.planning/research/PITFALLS.md` Pitfall 9 (restated in CONTEXT.md D-02) +
`scripts/verify_environment.py`'s own `except urllib.error.HTTPError` (non-retried) vs.
`except (URLError, OSError, ...)` (retried) split (lines 246-256).
**Apply to:** `oracle_partition_trigger.py`'s single `except oracledb.OperationalError` clause —
never widen to `oracledb.DatabaseError` or bare `oracledb.Error`/`Exception`.

### Single module-level logger, no per-function logger instances
**Source:** `airflow/dags/_common/oracle_partition_trigger.py` line 76,
`_LOGGER = logging.getLogger("airflow.task")`.
**Apply to:** Both new log lines (`warning` on retry, `debug` on guarded close-failure) — reuse
`_LOGGER`, do not instantiate a new logger.

### Constant-naming convention: `_UPPER_SNAKE` module-level, grouped near point of use
**Source:** `airflow/dags/_common/oracle_partition_trigger.py` lines 83-91
(`_POLL_QUERY`, `_BOTH_DATASETS_PRESENT`, each with a comment citing the design-doc rationale).
**Apply to:** The two new constants (`_MAX_TRANSIENT_RETRIES`, `_RETRY_BASE_DELAY_SECONDS`) —
place immediately after the existing two, each with a short comment citing the relevant CONTEXT.md
decision ID (D-03, D-05), matching the existing comment style exactly.

### Test mocking: mock the lowest boundary with `AsyncMock`, `patch` on the module's own import path
**Source:** `tests/unit/test_oracle_partition_trigger.py` docstring (lines 4-8) and existing
`patch("_common.oracle_partition_trigger.oracledb.connect_async", ...)` /
`patch("_common.oracle_partition_trigger.asyncio.sleep", ...)` calls.
**Apply to:** All 4 new D-08 test cases — patch at the same two import paths, never mock at a higher
level (e.g. mocking `OraclePartitionReadyTrigger.run` itself would defeat the point of the test).

## No Analog Found

None. Both files this phase touches have a strong in-repo analog (`verify_environment.py` for the
retry/backoff shape; the file's own existing tests for the test-extension shape).

## Metadata

**Analog search scope:** `airflow/dags/_common/`, `scripts/`, `tests/unit/` — no broader search
needed; CONTEXT.md and RESEARCH.md already named the exact analog (`scripts/verify_environment.py`)
and this phase touches exactly one source file + its one test file, so Step 3's search converged
immediately on the two files read above.
**Files scanned:** 3 (`oracle_partition_trigger.py`, `test_oracle_partition_trigger.py`,
`verify_environment.py`)
**Pattern extraction date:** 2026-09-02
