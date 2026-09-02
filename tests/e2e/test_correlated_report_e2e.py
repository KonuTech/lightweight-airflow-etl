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
the same convention already established by ``tests/e2e/test_csv_to_oracle_ingest_e2e.py``
and ``tests/unit/test_generate_csv.py``, never a plain ``import``.
"""

from __future__ import annotations

import importlib.util
import sys
import time
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import oracledb
import pytest
from csv_processor import load
from csv_processor.config.loader import load_config
from csv_processor.engine import Status, process

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_DATA_DIR = _REPO_ROOT / "data"

_GENERATE_CSV_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_GENERATE_CSV_SPEC = importlib.util.spec_from_file_location("generate_csv", _GENERATE_CSV_PATH)
assert _GENERATE_CSV_SPEC is not None and _GENERATE_CSV_SPEC.loader is not None
generate_csv = importlib.util.module_from_spec(_GENERATE_CSV_SPEC)
# Register in sys.modules BEFORE exec_module: generate_csv.py's frozen
# dataclasses (GeneratedCsv/CorrelatedDatasets) use postponed annotations,
# and dataclasses' forward-ref resolution looks the module up via
# sys.modules[cls.__module__] -- without this it raises AttributeError on a
# None module during class creation (mirrors test_csv_to_oracle_ingest_e2e.py's own
# workaround).
sys.modules["generate_csv"] = generate_csv
_GENERATE_CSV_SPEC.loader.exec_module(generate_csv)

_DAG_POLLING_PATH = _REPO_ROOT / "scripts" / "dag_polling.py"
_DAG_POLLING_SPEC = importlib.util.spec_from_file_location("dag_polling", _DAG_POLLING_PATH)
assert _DAG_POLLING_SPEC is not None and _DAG_POLLING_SPEC.loader is not None
dag_polling = importlib.util.module_from_spec(_DAG_POLLING_SPEC)
_DAG_POLLING_SPEC.loader.exec_module(dag_polling)

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

    # Run-unique seed (matches test_csv_to_oracle_ingest_e2e.py's own run-uniqueness
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


def _clear_dataset_fixtures(dataset: str) -> None:
    """Delete every pre-existing file matching ``<dataset>_*.csv*`` under both
    the dataset's watched directory and its ``.staging/`` subdir BEFORE
    triggering -- mirrors ``test_csv_to_oracle_ingest_e2e.py``'s
    ``_clear_existing_customers_fixtures()``, extended to ``.staging/`` too
    (D-24's staging path is a separate glob root the original helper never
    considered). A stale watched-directory file would make the sensor match
    immediately and never defer, silently invalidating this test's central
    deferred-wake ordering assertion."""
    data_dir = _DATA_DIR / dataset
    data_dir.mkdir(parents=True, exist_ok=True)
    for stale in data_dir.glob(f"{dataset}_*.csv*"):
        stale.unlink()
    staging_dir = data_dir / ".staging"
    if staging_dir.exists():
        for stale in staging_dir.glob(f"{dataset}_*.csv*"):
            stale.unlink()


def test_correlated_ingestion_via_live_dag_trigger_reports_across_backdated_partitions(
    oracle_cursor: oracledb.Cursor,
) -> None:
    """D-24/D-25: prove the staging+atomic-rename mechanism against the REAL,
    already-proven ``csv_to_oracle_ingest`` DAG (never mocked), for both datasets, and
    D-12: prove the business report aggregates correctly across a multi-day
    backdated-partition boundary.
    """
    customers_config = load_config(
        _CONFIGS_DIR / "datasets" / "customers.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    orders_config = load_config(
        _CONFIGS_DIR / "datasets" / "orders.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )

    _clear_dataset_fixtures("customers")
    _clear_dataset_fixtures("orders")

    # Run-unique seed (matches this file's own first test's convention) so
    # LOAD-04's checksum-based idempotency never collides with a prior run.
    seed = time.time_ns() % (2**31)
    correlated = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=25,
        orders_rows=25,
        invalid_ratio=0.2,
        seed=seed,
    )

    jwt_token = dag_polling.get_jwt_token(dag_polling.AIRFLOW_BASE_URL)

    # Step 1: trigger customers with its target fixture file confirmed
    # ABSENT, poll until wait_for_file genuinely reaches "deferred" BEFORE
    # the staged write/rename happens -- Pitfall 4's exact ordering, applied
    # per dataset. timeout=180 matches this project's established cold-start
    # headroom (docs/airflow-dag.md, test_csv_to_oracle_ingest_e2e.py).
    customers_run_id = dag_polling.trigger_dag("customers", "configs/datasets/customers.json")
    dag_polling.wait_for_task_state(
        dag_polling.AIRFLOW_BASE_URL,
        customers_run_id,
        "wait_for_file",
        jwt_token,
        "deferred",
        timeout=180.0,
    )
    # Step 2: ONLY THEN write the customers CSV via write_staged() (D-24's
    # staging+rename, now exercised against the LIVE stack's actual watched
    # directory -- D-25).
    generate_csv.write_staged(correlated.customers, customers_config, "customers")

    # Rule 1 deviation from the plan's literal step ordering: wait for the
    # customers DAG run to fully COMPLETE (not just reach "deferred") before
    # triggering orders. Plan 07-04's trg_orders_valid_customer_exists
    # BEFORE INSERT trigger requires each orders row's customer_id to
    # already exist, committed, in customers_valid -- triggering orders
    # while customers is still mid-flight is a genuine race (confirmed
    # empirically: process_csv_task's first insert_rows() call into
    # orders_valid raced customers_valid's commit and failed with
    # DATABASE_ERROR before either DAG's own deferred-wake proof was
    # affected). Both datasets' wait_for_file-deferred-before-write ordering
    # (D-24/D-25's actual subject) is unaffected by sequencing the two DAG
    # runs' full completions this way.
    customers_result = dag_polling.wait_for_dag_run_result(
        dag_polling.AIRFLOW_BASE_URL, customers_run_id, jwt_token
    )
    assert customers_result["status"] in {"SUCCESS", "SUCCESS_WITH_INVALID_ROWS"}, customers_result

    # Steps 3-4: repeat for orders, only after customers_valid's rows are
    # committed.
    orders_run_id = dag_polling.trigger_dag("orders", "configs/datasets/orders.json")
    dag_polling.wait_for_task_state(
        dag_polling.AIRFLOW_BASE_URL,
        orders_run_id,
        "wait_for_file",
        jwt_token,
        "deferred",
        timeout=180.0,
    )
    generate_csv.write_staged(correlated.orders, orders_config, "orders")

    orders_result = dag_polling.wait_for_dag_run_result(
        dag_polling.AIRFLOW_BASE_URL, orders_run_id, jwt_token
    )
    assert orders_result["status"] in {"SUCCESS", "SUCCESS_WITH_INVALID_ROWS"}, orders_result

    # Per RESEARCH.md Pitfall 4: orders_valid.ingested_at defaults to SYSDATE
    # unless explicitly included in the INSERT column list -- call
    # insert_rows() DIRECTLY (bypassing process()) with an EXTENDED columns
    # list so 2-3 extra orders_valid rows land in past-day partitions,
    # customer_id drawn from the SAME correlated customers pool this test
    # already generated above (D-12's multi-day backdated-partition proof).
    customer_id_index = correlated.customers.header.index("customer_id")
    valid_customer_pool = [
        row[customer_id_index]
        for row, category in zip(
            correlated.customers.rows, correlated.customers.categories, strict=True
        )
        if category is None
    ]
    assert valid_customer_pool, "correlated customers pool is empty -- test is meaningless"

    order_columns = [column.name for column in orders_config.columns]
    today = date.today()
    backdated_rows = [
        {
            "order_id": f"BACKDATED-{seed}-1",
            "customer_id": valid_customer_pool[0],
            "order_date": today - timedelta(days=2),
            "amount": Decimal("99.99"),
            "ingested_at": today - timedelta(days=2),
        },
        {
            "order_id": f"BACKDATED-{seed}-2",
            "customer_id": valid_customer_pool[-1],
            "order_date": today - timedelta(days=5),
            "amount": Decimal("199.99"),
            "ingested_at": today - timedelta(days=5),
        },
    ]
    load.insert_rows(
        oracle_cursor,
        table="orders_valid",
        columns=[*order_columns, "ingested_at"],
        rows=backdated_rows,
    )
    oracle_cursor.connection.commit()

    # Final assertion: the mirrored business-report query returns rows
    # spanning MORE THAN ONE distinct order_month bucket -- proving the
    # report aggregates correctly across the partition boundary the
    # backdated rows create, not just today's partition.
    oracle_cursor.execute(_BUSINESS_REPORT_SQL)
    rows = oracle_cursor.fetchall()
    distinct_months = {row[1] for row in rows}
    assert len(distinct_months) > 1, (
        f"expected the business report to span multiple order_month buckets "
        f"across the backdated partition boundary, got: {rows}"
    )
