"""Shared fixtures for ``csv_processor``'s real-Oracle integration suite
(TEST-02) -- every test under ``tests/integration/`` runs against the
actually-running Oracle Database Free container, never a mock.

Dev-only test code mirrors ``scripts/verify_environment.py``'s literal
``admin``/``admin`` defaults directly (``load.get_connection()`` already
falls back to them) -- no separate hardcoding needed.
"""

from __future__ import annotations

from collections.abc import Iterator

import oracledb
import pytest
from csv_processor import load


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
    test runs.

    Required because ``UNIQUE(dataset, checksum)`` (D-05) would otherwise
    make a second test run collide with a prior run's leftover row. These
    tables are ``INTERVAL``-partitioned but plain ``DELETE`` still works
    (04-RESEARCH.md).
    """
    oracle_cursor.execute("DELETE FROM customers_valid")
    oracle_cursor.execute("DELETE FROM customers_invalid")
    oracle_cursor.execute("DELETE FROM ingestion_metadata WHERE dataset = 'customers'")
    oracle_cursor.connection.commit()
