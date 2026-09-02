---
phase: 10-oraclepartitionreadytrigger-robustness-fix
reviewed: 2026-09-02T06:34:52Z
depth: standard
files_reviewed: 3
files_reviewed_list:
  - airflow/dags/_common/oracle_partition_trigger.py
  - tests/unit/test_oracle_partition_trigger.py
  - Makefile
findings:
  critical: 0
  warning: 2
  info: 3
  total: 5
status: issues_found
---

# Phase 10: Code Review Report

**Reviewed:** 2026-09-02T06:34:52Z
**Depth:** standard
**Files Reviewed:** 3
**Status:** issues_found

## Summary

Reviewed `OraclePartitionReadyTrigger.run()`'s bounded retry/backoff fix, its extended unit test
suite, and the `Makefile`'s `verify-phase10` target. This is a well-scoped, well-researched fix:
cross-checked against `.planning/research/PITFALLS.md` (Pitfall 9/10, including a live-verified
`oracledb==4.0.2` exception hierarchy) and `.planning/phases/10-.../10-CONTEXT.md`'s recorded
decisions (D-01 through D-08). Traced the full control flow by hand (connect failure before/after
`connection` assignment, cursor-level failure inside the inner `try`, `finally`-guarded
`connection.close()`, retry-counter reset-on-success, exponential backoff formula, exhausted-retry
re-raise) and ran the actual test suite plus `ruff check`/`mypy` against the reviewed files — all
green, no regressions found. The four `D-08` test scenarios (retry-then-succeed, exhaust-then-raise,
non-transient-immediate, close-failure-does-not-mask) are all present and pass.

No BLOCKER-level defects found. Two WARNING-level gaps and three INFO-level quality notes below —
none of them contradict this phase's documented design decisions; they're either untested-but-real
edge cases in the new retry logic, or pre-existing (out-of-phase-scope) quality nits in code this
phase's diff otherwise leaves untouched.

## Warnings

### WR-01: Exponential backoff delay values are exercised but never asserted

**File:** `tests/unit/test_oracle_partition_trigger.py:113-137`
**Issue:** `test_run_retries_transient_operational_error_then_succeeds` only asserts
`mock_sleep.assert_awaited()` (at least one await, any argument) rather than the actual delay value.
No test in the file asserts the specific values produced by the exponential-backoff formula in
`airflow/dags/_common/oracle_partition_trigger.py:144-147`
(`min(_RETRY_BASE_DELAY_SECONDS * (2 ** (retry_count - 1)), self.poke_interval)`), nor the number of
times `asyncio.sleep` is invoked during a multi-failure retry sequence. This formula and its
"capped at `poke_interval`" behavior is one of this phase's own core documented decisions (D-05 in
`10-CONTEXT.md`), yet a regression in it (e.g. swapping `2 ** (retry_count - 1)` for
`2 ** retry_count`, or dropping the `min(...)` cap) would not fail any existing test.
**Fix:** Add an explicit assertion on the sleep call sequence, e.g. in a dedicated test:
```python
def test_run_backoff_delay_doubles_and_is_capped_at_poke_interval() -> None:
    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(side_effect=[oracledb.OperationalError("x")] * 3 + [_mock_connection([(2,)])]),
        ),
        patch(
            "_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)
        ) as mock_sleep,
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=2.5)
        asyncio.run(_collect_events(trigger))

    mock_sleep.assert_has_awaits([call(1.0), call(2.0), call(2.5)])  # capped at poke_interval
```

### WR-02: `connection.close()` failures inside `finally` are only logged at `debug`, easy to lose in production monitoring

**File:** `airflow/dags/_common/oracle_partition_trigger.py:132-139`
**Issue:** This matches Pitfall 10's documented fix exactly (never re-raise/mask the original
exception), but `DEBUG` is typically filtered out of default production log levels/alerting.
Combined with the fact that this branch only fires when a connection was *already broken*
(the `connection.close()` call on an already-severed session is itself evidence something is
wrong at the network/DB layer), silently dropping it to `debug` means an operator who isn't
running the triggerer at `DEBUG` verbosity never sees this signal — they'd only see the (correct,
more diagnostic) original exception, with no trace that the connection's teardown also failed.
This is a real signal, not noise; `debug` under-reports it relative to its severity.
**Fix:** Keep the guard (never re-raise/mask) but raise the log level, e.g. `_LOGGER.warning(...)`
instead of `_LOGGER.debug(...)`, so it's visible at the same operational tier as the retry-attempt
warnings a few lines above (`airflow/dags/_common/oracle_partition_trigger.py:148-154`):
```python
    finally:
        try:
            await connection.close()
        except oracledb.Error:
            _LOGGER.warning(
                "connection.close() failed on an already-broken connection",
                exc_info=True,
            )
```

## Info

### IN-01: `ReportReadySensor` has zero test coverage

**File:** `airflow/dags/_common/oracle_partition_trigger.py:165-176`
**Issue:** `tests/unit/test_oracle_partition_trigger.py`'s own module docstring claims to be
"unit coverage for `airflow/dags/_common/oracle_partition_trigger.py`," but no test exercises
`ReportReadySensor.execute()` (does it call `self.defer(...)` with the right `trigger`/
`method_name`?) or `execute_complete()`. This predates Phase 10 (introduced in `4322f5f`,
`ReportReadySensor` is explicitly out of this phase's scope per `10-CONTEXT.md`'s
`<code_context>` section), so it is not a regression from this fix — flagged only because it's a
real, currently-unverified gap in a file this phase otherwise hardens.
**Fix:** A follow-up test could assert `sensor.defer` is called with
`trigger=OraclePartitionReadyTrigger(...)` and `method_name="execute_complete"` via a mock on
`self.defer`, e.g. by patching `_FallbackBaseSensorOperator.defer` (or the real one when running
inside the Airflow container).

### IN-02: `ReportReadySensor`'s hardcoded `poke_interval=30` duplicates the trigger's own default with no single source of truth

**File:** `airflow/dags/_common/oracle_partition_trigger.py:111,171`
**Issue:** `OraclePartitionReadyTrigger.__init__(self, poke_interval: float = 30.0)` and
`ReportReadySensor.execute()`'s `OraclePartitionReadyTrigger(poke_interval=30)` both hardcode `30`
independently, and `ReportReadySensor` exposes no way for a DAG author to override it. If one is
changed without the other, they silently drift (the sensor's literal is currently `30`, an `int`,
vs. the trigger's own default `30.0`, a `float` — functionally equivalent but a symptom of the
duplication).
**Fix:** Either drop the trigger instantiation's explicit argument so it relies on the class
default, or thread `poke_interval` through `ReportReadySensor.__init__` so it's genuinely
configurable per-DAG instead of a second hardcoded copy:
```python
class ReportReadySensor(BaseSensorOperator):
    def __init__(self, *, poke_interval: float = 30.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval

    def execute(self, context: object) -> None:
        self.defer(
            trigger=OraclePartitionReadyTrigger(poke_interval=self.poke_interval),
            method_name="execute_complete",
        )
```

### IN-03: `(count,) = await cursor.fetchone()` assumes exactly one column, unguarded against a `None`/shape mismatch

**File:** `airflow/dags/_common/oracle_partition_trigger.py:131`
**Issue:** `_POLL_QUERY` is a `SELECT COUNT(...)` with no `GROUP BY`, so it is expected to always
return exactly one row with exactly one column — `fetchone()` returning `None`, or a driver
returning a differently-shaped row, is not realistically reachable today. If it ever did happen
(e.g. the query is edited later to add a `GROUP BY` without updating this unpacking), the resulting
`TypeError`/`ValueError` is a plain Python exception, not an `oracledb.Error` subclass, so it would
bypass the retry logic entirely and propagate immediately from `run()` — arguably correct
("genuine bug, don't retry"), but worth a one-line comment given the effort already spent
elsewhere in this module explaining exception-scope choices.
**Fix:** Not required; consider a short comment near `_POLL_QUERY`/the unpacking noting the
single-row/single-column assumption so a future edit doesn't silently break it.

---

_Reviewed: 2026-09-02T06:34:52Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
