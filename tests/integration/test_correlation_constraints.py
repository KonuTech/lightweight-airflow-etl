"""Live-Oracle proof for the correlation DDL (DB-01, DB-02, D-13 through D-18,
07-04-PLAN.md) -- every test in this file runs against the actually running
Oracle Database Free container (``make up``), never a mock.

Proves, directly via ``load.insert_rows()`` (never through
``csv_processor.engine.process_chunks()``, since these are DB-level
constraints unrelated to CSV parsing):

1. ``customers_valid``/``orders_valid`` each reject a duplicate primary key.
2. ``orders_valid`` rejects an INSERT whose ``customer_id`` does not exist in
   ``customers_valid`` (the ``trg_orders_valid_customer_exists`` trigger),
   and the whole batch fails -- zero rows land (D-16).
3. A legitimate correlated ``orders_valid`` insert (a real, existing
   ``customer_id``) is unaffected by the trigger.
4. ``customers_invalid``/``orders_invalid`` remain fully unconstrained (D-18)
   -- a blank ``customer_id`` and an unmatched FK-shaped value are both
   accepted without error.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from decimal import Decimal

import oracledb
import pytest
from csv_processor import load

_CUSTOMERS_VALID_COLUMNS = [
    "customer_id",
    "name",
    "country",
    "birth_date",
    "event_ts",
    "signup_country",
]
_ORDERS_VALID_COLUMNS = ["order_id", "customer_id", "order_date", "amount"]
_CUSTOMERS_INVALID_COLUMNS = _CUSTOMERS_VALID_COLUMNS + list(load.INVALID_ROW_SUFFIX_COLUMNS)
_ORDERS_INVALID_COLUMNS = _ORDERS_VALID_COLUMNS + list(load.INVALID_ROW_SUFFIX_COLUMNS)


@pytest.fixture(autouse=True)
def clean_orders_tables(oracle_cursor: oracledb.Cursor) -> Iterator[None]:
    """Delete leftover ``orders_valid``/``orders_invalid``/``ingestion_metadata``
    (``orders`` dataset) rows BEFORE and AFTER each test -- mirrors
    ``conftest.py``'s ``clean_customers_tables`` shape. ``clean_customers_tables``
    (autouse in ``conftest.py``) already handles the customers side.

    Runs the cleanup both before AND after each test (not just before) because
    a trigger-rejected INSERT in this file's own tests leaves the connection
    mid-transaction; cleaning up after guarantees no leftover row survives into
    the next test regardless of test order.
    """
    oracle_cursor.execute("DELETE FROM orders_valid")
    oracle_cursor.execute("DELETE FROM orders_invalid")
    oracle_cursor.execute("DELETE FROM ingestion_metadata WHERE dataset = 'orders'")
    oracle_cursor.connection.commit()
    yield
    oracle_cursor.execute("DELETE FROM orders_valid")
    oracle_cursor.execute("DELETE FROM orders_invalid")
    oracle_cursor.connection.commit()


def _customer_row(customer_id: str) -> dict[str, object]:
    return {
        "customer_id": customer_id,
        "name": "Test Customer",
        "country": "US",
        "birth_date": dt.date(1990, 1, 1),
        "event_ts": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        "signup_country": "US",
    }


def _order_row(order_id: str, customer_id: str) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": dt.date(2026, 1, 1),
        "amount": Decimal("10.00"),
    }


def test_duplicate_customer_id_violates_primary_key(oracle_cursor: oracledb.Cursor) -> None:
    load.insert_rows(
        oracle_cursor,
        table="customers_valid",
        columns=_CUSTOMERS_VALID_COLUMNS,
        rows=[_customer_row("CUST_DUP")],
    )
    oracle_cursor.connection.commit()

    with pytest.raises(oracledb.IntegrityError) as exc_info:
        load.insert_rows(
            oracle_cursor,
            table="customers_valid",
            columns=_CUSTOMERS_VALID_COLUMNS,
            rows=[_customer_row("CUST_DUP")],
        )
    (error_obj,) = exc_info.value.args
    assert error_obj.full_code == "ORA-00001"
    oracle_cursor.connection.rollback()


def test_orders_valid_insert_with_unknown_customer_id_is_rejected(
    oracle_cursor: oracledb.Cursor,
) -> None:
    load.insert_rows(
        oracle_cursor,
        table="customers_valid",
        columns=_CUSTOMERS_VALID_COLUMNS,
        rows=[_customer_row("CUST_KNOWN_1")],
    )
    oracle_cursor.connection.commit()

    with pytest.raises(oracledb.DatabaseError) as exc_info:
        load.insert_rows(
            oracle_cursor,
            table="orders_valid",
            columns=_ORDERS_VALID_COLUMNS,
            rows=[_order_row("ORD_REJECTED", "CUST_DOES_NOT_EXIST")],
        )
    (error_obj,) = exc_info.value.args
    assert error_obj.full_code == "ORA-20001"
    oracle_cursor.connection.rollback()

    # D-16: the whole batch/chunk fails -- zero rows land, no partial insert.
    oracle_cursor.execute(
        "SELECT COUNT(*) FROM orders_valid WHERE order_id = :order_id",
        {"order_id": "ORD_REJECTED"},
    )
    (count,) = oracle_cursor.fetchone()
    assert count == 0


def test_orders_valid_insert_with_known_customer_id_succeeds(
    oracle_cursor: oracledb.Cursor,
) -> None:
    load.insert_rows(
        oracle_cursor,
        table="customers_valid",
        columns=_CUSTOMERS_VALID_COLUMNS,
        rows=[_customer_row("CUST_KNOWN_2")],
    )
    oracle_cursor.connection.commit()

    load.insert_rows(
        oracle_cursor,
        table="orders_valid",
        columns=_ORDERS_VALID_COLUMNS,
        rows=[_order_row("ORD_ACCEPTED", "CUST_KNOWN_2")],
    )
    oracle_cursor.connection.commit()

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM orders_valid WHERE order_id = :order_id", {"order_id": "ORD_ACCEPTED"}
    )
    (count,) = oracle_cursor.fetchone()
    assert count == 1


def test_invalid_tables_accept_blank_customer_id_unconstrained(
    oracle_cursor: oracledb.Cursor,
) -> None:
    # D-18: customers_invalid/orders_invalid never carry a PK, index, or
    # trigger -- a blank customer_id (missing_required's real output shape)
    # and an orders_invalid row referencing no real customer both succeed.
    customer_invalid_row = {
        "customer_id": "",
        "name": "",
        "country": "US",
        "birth_date": "",
        "event_ts": "",
        "signup_country": "US",
        "error_code": "NULL_VIOLATION",
        "error_message": "customer_id is required",
        "source_file": "customers_test.csv",
        "row_number": 1,
        "raw_line": ",,US,,,US",
    }
    order_invalid_row = {
        "order_id": "ORD_INVALID_1",
        "customer_id": "CUST_NEVER_EXISTED",
        "order_date": "2026-01-01",
        "amount": "10.00",
        "error_code": "TYPE_MISMATCH",
        "error_message": "unrelated to correlation",
        "source_file": "orders_test.csv",
        "row_number": 1,
        "raw_line": "ORD_INVALID_1,CUST_NEVER_EXISTED,2026-01-01,10.00",
    }

    load.insert_rows(
        oracle_cursor,
        table="customers_invalid",
        columns=_CUSTOMERS_INVALID_COLUMNS,
        rows=[customer_invalid_row],
    )
    load.insert_rows(
        oracle_cursor,
        table="orders_invalid",
        columns=_ORDERS_INVALID_COLUMNS,
        rows=[order_invalid_row],
    )
    oracle_cursor.connection.commit()

    # Oracle treats an empty VARCHAR2 as NULL (`= ''` never matches NULL), so
    # this asserts via IS NULL and the row's own source_file marker, not `= ''`.
    oracle_cursor.execute(
        "SELECT COUNT(*) FROM customers_invalid WHERE customer_id IS NULL "
        "AND source_file = :source_file",
        {"source_file": "customers_test.csv"},
    )
    (customers_count,) = oracle_cursor.fetchone()
    assert customers_count == 1

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM orders_invalid WHERE order_id = :order_id",
        {"order_id": "ORD_INVALID_1"},
    )
    (orders_count,) = oracle_cursor.fetchone()
    assert orders_count == 1
