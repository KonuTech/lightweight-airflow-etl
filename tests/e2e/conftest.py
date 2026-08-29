"""Shared fixtures for the automated end-to-end suite (TEST-03, D-08) --
every test under ``tests/e2e/`` runs against the real, already-running
docker-compose stack (``make up`` first), never a mock.

``oracle_cursor``/``clean_customers_tables`` mirror
``tests/integration/conftest.py``'s exact shape (06-PATTERNS.md); the new
``airflow_stack_reachable`` fixture fails loudly, with a clear message,
if the stack was never brought up first, mirroring
``docker-compose.yml``'s own ``airflow-apiserver`` healthcheck target.
"""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request
from typing import Iterator

import oracledb
import pytest

from csv_processor import load

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")

_HEALTH_CHECK_TIMEOUT_SECONDS = 30.0
_HEALTH_CHECK_INTERVAL_SECONDS = 2.0


@pytest.fixture(scope="session", autouse=True)
def airflow_stack_reachable() -> None:
    """Poll ``GET {AIRFLOW_BASE_URL}/api/v2/monitor/health`` with a bounded
    timeout -- fails loudly (a clear ``pytest.fail`` message, never a raw
    connection-refused traceback) if the stack was never brought up
    (``make up`` first, this plan's own ``<precondition>``)."""
    url = f"{AIRFLOW_BASE_URL}/api/v2/monitor/health"
    deadline = time.monotonic() + _HEALTH_CHECK_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError) as exc:
            last_error = exc
        time.sleep(_HEALTH_CHECK_INTERVAL_SECONDS)
    pytest.fail(
        f"Airflow stack at {AIRFLOW_BASE_URL} never became healthy within "
        f"{_HEALTH_CHECK_TIMEOUT_SECONDS}s -- run `make up` first (matches "
        f"verify-phase4/verify-phase5's own convention). Last error: {last_error}"
    )


@pytest.fixture
def oracle_cursor() -> Iterator[oracledb.Cursor]:
    """A real Oracle cursor -- commits and closes the connection on teardown."""
    connection = load.get_connection()
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        connection.commit()
        cursor.close()
        connection.close()


@pytest.fixture(autouse=True)
def clean_customers_tables(oracle_cursor: oracledb.Cursor) -> None:
    """Delete leftover ``customers_valid``/``customers_invalid``/
    ``ingestion_metadata`` rows for the ``customers`` dataset BEFORE each
    e2e test runs.

    Required because ``UNIQUE(dataset, checksum)`` (LOAD-04) would otherwise
    make a second e2e run collide with a prior run's leftover row -- same
    pattern as ``tests/integration/conftest.py``'s ``clean_customers_tables``,
    applied here too.
    """
    oracle_cursor.execute("DELETE FROM customers_valid")
    oracle_cursor.execute("DELETE FROM customers_invalid")
    oracle_cursor.execute("DELETE FROM ingestion_metadata WHERE dataset = 'customers'")
    oracle_cursor.connection.commit()
