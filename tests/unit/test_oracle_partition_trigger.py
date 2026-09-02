"""Fast, no-network unit coverage for
``airflow/dags/_common/oracle_partition_trigger.py`` (07-03-PLAN.md Task 1).

Mocks ``oracledb.connect_async`` and ``asyncio.sleep`` entirely -- both
`AsyncMock`s, per this module's own "mock the lowest boundary" convention
(mirrors Phase 6's ``urlopen``-mocking discipline for ``test_dag_polling.py``,
applied here to Oracle's async driver) -- so this test runs instantly with no
real Oracle connection, no real Airflow, and no real sleep.

``_common`` (``airflow/dags/_common/``) is only importable via ``sys.path``
insertion (``tests/unit/dags/conftest.py``'s established convention), never
the project's normal ``pythonpath`` (repo root only).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import oracledb
import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.oracle_partition_trigger import (  # noqa: E402
    OraclePartitionReadyTrigger,
    TriggerEvent,
)


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


def test_serialize_returns_the_expected_classpath_and_poke_interval() -> None:
    trigger = OraclePartitionReadyTrigger(poke_interval=5.0)

    assert trigger.serialize() == (
        "_common.oracle_partition_trigger.OraclePartitionReadyTrigger",
        {"poke_interval": 5.0},
    )


def test_run_does_not_yield_when_only_one_dataset_is_present() -> None:
    """First poll: `fetchone()` yields `(1,)` -- the trigger must sleep and
    poll again, never yielding a `TriggerEvent` yet."""
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


def test_run_yields_exactly_one_trigger_event_once_both_datasets_present() -> None:
    """Second poll: `fetchone()` yields `(2,)` (both datasets present) --
    the trigger yields exactly one `TriggerEvent({"status": "ready"})` and
    returns."""
    connection = _mock_connection([(1,), (2,)])

    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(return_value=connection),
        ),
        patch("_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)),
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=0.01)
        events = asyncio.run(_collect_events(trigger))

    assert len(events) == 1
    assert isinstance(events[0], TriggerEvent)
    assert events[0].payload == {"status": "ready"}


def test_poll_query_uses_real_wall_clock_date_never_logical_date_or_data_interval() -> None:
    """D-29: a literal string-content assertion on the SQL the trigger
    executes -- must reference `TRUNC(SYSDATE)`, never
    `logical_date`/`data_interval`."""
    from _common.oracle_partition_trigger import _POLL_QUERY

    assert "TRUNC(SYSDATE)" in _POLL_QUERY
    assert "logical_date" not in _POLL_QUERY
    assert "data_interval" not in _POLL_QUERY


def test_run_retries_transient_operational_error_then_succeeds() -> None:
    """D-08a: a single transient `OperationalError` from `connect_async()` is
    retried (with backoff), then a successful connection yields the ready
    event -- the retry logic recovers rather than crashing the sensor."""
    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(
                side_effect=[
                    oracledb.OperationalError("DPY-6005: cannot connect"),
                    _mock_connection([(2,)]),
                ]
            ),
        ),
        patch(
            "_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)
        ) as mock_sleep,
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=30.0)
        events = asyncio.run(_collect_events(trigger))

    assert len(events) == 1
    assert events[0].payload == {"status": "ready"}
    mock_sleep.assert_awaited()


def test_run_backoff_delay_doubles_and_is_capped_at_poke_interval() -> None:
    """D-05: the exponential backoff formula
    (`min(_RETRY_BASE_DELAY_SECONDS * (2 ** (retry_count - 1)), poke_interval)`)
    doubles the delay each consecutive transient failure and caps it at
    `poke_interval` -- asserted by exact value/sequence so a regression in
    the formula (wrong exponent base, dropped cap) fails this test even
    though other retry tests only check `assert_awaited()`."""
    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(
                side_effect=[oracledb.OperationalError("x")] * 3 + [_mock_connection([(2,)])]
            ),
        ),
        patch(
            "_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)
        ) as mock_sleep,
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=2.5)
        asyncio.run(_collect_events(trigger))

    mock_sleep.assert_has_awaits([call(1.0), call(2.0), call(2.5)])  # capped at poke_interval


def test_run_reraises_after_exhausting_transient_retries() -> None:
    """D-08b: 11 consecutive `OperationalError` failures (one more than
    `_MAX_TRANSIENT_RETRIES`) exhausts the retry budget -- the original
    exception propagates out of `run()` uncaught (D-04)."""
    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(side_effect=[oracledb.OperationalError("DPY-6005: cannot connect")] * 11),
        ),
        patch("_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)),
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=30.0)
        with pytest.raises(oracledb.OperationalError):
            asyncio.run(_collect_events(trigger))


def test_run_propagates_non_transient_error_immediately() -> None:
    """D-08c: a non-transient `oracledb.Error` subclass (e.g.
    `ProgrammingError` from a bad query or dropped/renamed table) is never
    caught for retry -- it propagates immediately on first occurrence, and
    no backoff sleep is ever attempted (D-02)."""
    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(side_effect=oracledb.ProgrammingError("ORA-00904: invalid identifier")),
        ),
        patch(
            "_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)
        ) as mock_sleep,
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=30.0)
        with pytest.raises(oracledb.ProgrammingError):
            asyncio.run(_collect_events(trigger))

    mock_sleep.assert_not_awaited()


def test_run_close_failure_does_not_mask_original_exception() -> None:
    """D-08d: a `connection.close()` failure inside `finally` never masks
    the original exception that was already propagating from the try block
    -- even after the retry cap is exhausted, the exception ultimately
    raised is the original `OperationalError`, never the close-time
    `oracledb.Error` (D-06)."""
    connection = _mock_connection([])
    connection.cursor.return_value.execute = AsyncMock(
        side_effect=[oracledb.OperationalError("DPY-4011: connection closed")] * 11
    )
    connection.close = AsyncMock(side_effect=oracledb.Error("close failed"))

    with (
        patch(
            "_common.oracle_partition_trigger.oracledb.connect_async",
            AsyncMock(return_value=connection),
        ),
        patch("_common.oracle_partition_trigger.asyncio.sleep", AsyncMock(return_value=None)),
    ):
        trigger = OraclePartitionReadyTrigger(poke_interval=30.0)
        with pytest.raises(oracledb.OperationalError):
            asyncio.run(_collect_events(trigger))
