"""Automated end-to-end proof for D-09/D-10: a real, un-mocked Oracle
``customers`` JOIN ``orders`` returns real rows for the first time in this
project's history, because ``orders.customer_id`` is now a genuine,
Zipf-weighted, with-replacement sample from the pool of ``customer_id``
values that land in ``customers_valid`` (``generate_correlated_datasets()``,
``generator/generate_csv.py``).

This is Phase 7's tracer (07-01-PLAN.md Task 1) -- production-quality, the
permanent e2e regression test for D-09/D-10, not a throwaway prototype.
Requires the full docker-compose stack (``make up``) already running and
healthy (this file's own ``<precondition>``).

Bypasses the Airflow DAG/FileSensor layer entirely on purpose: this test
proves the correlation logic + the real Oracle JOIN via
``csv_processor.engine.process()`` directly, Phase 4's already-proven
entrypoint. Proving this same data flows correctly through the live Airflow
DAG is a separate, later concern (D-24/D-25), not this test's.

``generator/generate_csv.py`` has no ``__init__.py`` and is not an installed
package, so it is loaded via ``importlib.util.spec_from_file_location`` --
the same convention already established by ``tests/e2e/test_csv_ingest_e2e.py``
and ``tests/unit/test_generate_csv.py``, never a plain ``import``.
"""

from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import oracledb
import pytest
from csv_processor.config.loader import load_config
from csv_processor.engine import Status, process

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"

_GENERATE_CSV_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_GENERATE_CSV_SPEC = importlib.util.spec_from_file_location("generate_csv", _GENERATE_CSV_PATH)
assert _GENERATE_CSV_SPEC is not None and _GENERATE_CSV_SPEC.loader is not None
generate_csv = importlib.util.module_from_spec(_GENERATE_CSV_SPEC)
# Register in sys.modules BEFORE exec_module: generate_csv.py's frozen
# dataclasses (GeneratedCsv/CorrelatedDatasets) use postponed annotations,
# and dataclasses' forward-ref resolution looks the module up via
# sys.modules[cls.__module__] -- without this it raises AttributeError on a
# None module during class creation (mirrors test_csv_ingest_e2e.py's own
# workaround).
sys.modules["generate_csv"] = generate_csv
_GENERATE_CSV_SPEC.loader.exec_module(generate_csv)

# Verbatim from scripts/verify_evidence.sql's business-report SELECT
# (lines 57-66) -- never re-authored a third time, per this project's
# established "never re-author twice" discipline.
_BUSINESS_REPORT_SQL = """
SELECT
    c.country AS region,
    TRUNC(o.order_date, 'MM') AS order_month,
    COUNT(*) AS order_count,
    SUM(o.amount) AS total_amount,
    ROUND(AVG(o.amount), 2) AS avg_amount
FROM customers_valid c
JOIN orders_valid o ON o.customer_id = c.customer_id
GROUP BY c.country, TRUNC(o.order_date, 'MM')
ORDER BY region, order_month
"""


@pytest.fixture(autouse=True)
def clean_orders_tables(oracle_cursor: oracledb.Cursor) -> None:
    """Delete leftover ``orders_valid``/``orders_invalid``/
    ``ingestion_metadata`` rows for the ``orders`` dataset BEFORE each e2e
    test runs -- mirrors ``tests/e2e/conftest.py``'s ``clean_customers_tables``
    exact shape, applied to the orders side (which the shared conftest does
    not already clean)."""
    oracle_cursor.execute("DELETE FROM orders_valid")
    oracle_cursor.execute("DELETE FROM orders_invalid")
    oracle_cursor.execute("DELETE FROM ingestion_metadata WHERE dataset = 'orders'")
    oracle_cursor.connection.commit()


def test_correlated_customers_orders_join_returns_at_least_one_row(
    oracle_cursor: oracledb.Cursor,
) -> None:
    customers_config = load_config(
        _CONFIGS_DIR / "datasets" / "customers.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    orders_config = load_config(
        _CONFIGS_DIR / "datasets" / "orders.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )

    # Run-unique seed (matches test_csv_ingest_e2e.py's own run-uniqueness
    # convention) so LOAD-04's checksum-based idempotency never collides
    # with a prior test run.
    seed = time.time_ns() % (2**31)
    correlated = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=30,
        orders_rows=60,
        invalid_ratio=0.2,
        seed=seed,
    )

    unique_suffix = time.time_ns()
    customers_path = generate_csv.output_path("customers").with_name(
        f"customers_{unique_suffix}.csv"
    )
    orders_path = generate_csv.output_path("orders").with_name(f"orders_{unique_suffix}.csv")
    generate_csv.write_csv(correlated.customers, customers_config, customers_path)
    generate_csv.write_csv(correlated.orders, orders_config, orders_path)

    customers_result = process(customers_path, customers_config)
    assert customers_result.status in {Status.SUCCESS, Status.SUCCESS_WITH_INVALID_ROWS}, (
        customers_result
    )

    orders_result = process(orders_path, orders_config)
    assert orders_result.status in {Status.SUCCESS, Status.SUCCESS_WITH_INVALID_ROWS}, orders_result

    oracle_cursor.execute(_BUSINESS_REPORT_SQL)
    rows = oracle_cursor.fetchall()
    assert len(rows) >= 1, "customers JOIN orders business report returned zero rows"
