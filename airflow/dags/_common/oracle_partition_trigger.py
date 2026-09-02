"""Custom deferrable Oracle-polling trigger + sensor for the ``report_ready``
DAG (D-26/D-28).

``apache-airflow-providers-oracle==4.6.2`` (already pinned, see
``docker/airflow/Dockerfile``) ships exactly one operator
(``SQLExecuteQueryOperator``) -- no sensor, no deferrable operator of any
kind (07-RESEARCH.md, confirmed against the provider's own official docs and
Airflow's "Supported Deferrable Operators" reference). This module is the
only path to a non-blocking "wait until both ``customers`` and ``orders``
have ingested data for today's real wall-clock-date partition" wait.

``OraclePartitionReadyTrigger.run()`` uses ``oracledb.connect_async()``,
never the blocking ``oracledb.connect()`` -- the triggerer runs a single
shared asyncio event loop serving every deferred task across every DAG
project-wide (T-07-03-01); a blocking call here would stall that loop for
every other deferred trigger too, not just this one.

``apache-airflow``/``apache-airflow-providers-*`` are deliberately NOT a
dependency of this project's root ``pyproject.toml`` (Phase 5's recorded
architectural boundary -- Airflow only ever runs inside
``docker/airflow/Dockerfile``'s container image, keeping every contributor's
local dev environment and this project's ``lint-type-unit`` CI job free of a
~200MB dependency). ``tests/unit/test_oracle_partition_trigger.py`` must
therefore run in an environment with no real ``airflow`` package installed
at all -- the ``try``/``except ModuleNotFoundError`` fallback below provides
a minimal structural stand-in for ``BaseTrigger``/``TriggerEvent``/
``BaseSensorOperator`` so this module (and its own polling logic) stays
unit-testable locally without reversing that boundary. Inside the real
Airflow container -- where ``apache-airflow`` is always installed -- the
``try`` branch always succeeds and the real framework classes are used.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import oracledb
from csv_processor.load import oracle_dsn, oracle_password, oracle_user

try:  # pragma: no cover -- exercised for real only inside the Airflow container
    from airflow.sdk import BaseSensorOperator
    from airflow.triggers.base import BaseTrigger, TriggerEvent
except ModuleNotFoundError:  # local/CI venv: apache-airflow deliberately absent

    class _FallbackBaseTrigger:
        """Minimal structural stand-in for ``airflow.triggers.base.BaseTrigger``,
        used only when ``apache-airflow`` is not installed (see module
        docstring)."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    class _FallbackTriggerEvent:
        """Minimal structural stand-in for ``airflow.triggers.base.TriggerEvent``."""

        def __init__(self, payload: Any) -> None:
            self.payload = payload

    class _FallbackBaseSensorOperator:
        """Minimal structural stand-in for ``airflow.sdk.BaseSensorOperator``."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def defer(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
            msg = "defer() is only meaningful inside a real Airflow worker process"
            raise NotImplementedError(msg)

    BaseTrigger = _FallbackBaseTrigger
    TriggerEvent = _FallbackTriggerEvent
    BaseSensorOperator = _FallbackBaseSensorOperator

_LOGGER = logging.getLogger("airflow.task")

# D-29: the real wall-clock date, directly -- never any Airflow DAG-run
# scheduling-timestamp arithmetic. This project's DAGs are always manually/
# API-triggered, and Airflow 3's manually-triggered runs do not guarantee
# those scheduling timestamps derive from one another at all (07-RESEARCH.md,
# [VERIFIED via Context7 apache/airflow docs]).
_POLL_QUERY = (
    "SELECT COUNT(DISTINCT dataset) FROM ingestion_metadata "
    "WHERE dataset IN ('customers', 'orders') "
    "AND TRUNC(processed_at) = TRUNC(SYSDATE)"
)

# Both `customers` and `orders` must have at least one ingestion_metadata row
# for today's partition before the sensor is satisfied (D-28).
_BOTH_DATASETS_PRESENT = 2

# Bounded retry cap for consecutive transient oracledb.OperationalError
# failures during a poll cycle (D-03) -- after this many consecutive
# failures, the original exception is re-raised rather than retried forever.
_MAX_TRANSIENT_RETRIES = 10

# Base delay (seconds) for the exponential backoff between retries, capped at
# `self.poke_interval` (D-05) -- mirrors scripts/verify_environment.py's own
# retry-backoff convention, adapted to `asyncio.sleep` (never `time.sleep`).
_RETRY_BASE_DELAY_SECONDS = 1.0


class OraclePartitionReadyTrigger(BaseTrigger):
    """Polls Oracle's ``ingestion_metadata`` table directly (D-28) -- the
    actual source of truth for "has data arrived" -- until both
    ``customers`` and ``orders`` have a row for today's real wall-clock-date
    partition (D-29), then yields exactly one ``TriggerEvent`` and returns.
    """

    def __init__(self, poke_interval: float = 30.0) -> None:
        super().__init__()
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "_common.oracle_partition_trigger.OraclePartitionReadyTrigger",
            {"poke_interval": self.poke_interval},
        )

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
                        _LOGGER.warning(
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


class ReportReadySensor(BaseSensorOperator):
    """Defers immediately to ``OraclePartitionReadyTrigger`` (D-26/D-28) --
    never occupies a worker slot while waiting for both datasets."""

    def execute(self, context: object) -> None:
        self.defer(
            trigger=OraclePartitionReadyTrigger(poke_interval=30),
            method_name="execute_complete",
        )

    def execute_complete(self, context: object, event: dict[str, Any] | None = None) -> None:
        return
