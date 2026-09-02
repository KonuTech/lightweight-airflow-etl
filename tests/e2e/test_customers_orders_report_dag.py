"""Live proof for D-26/D-28/D-29: the ``customers_orders_report`` DAG's
``wait_for_both_datasets`` sensor genuinely defers until BOTH ``customers``
and ``orders`` have ingestion for today's real wall-clock-date partition,
then ``build_report_task`` runs and the DAG completes successfully.

Bypasses the ``csv_to_oracle_ingest`` DAG/``FileSensor`` layer on purpose for the
ingestion side (mirrors ``tests/e2e/test_correlated_report_e2e.py``'s own
choice) -- this test's own concern is proving the NEW ``customers_orders_report`` DAG's
sensor waits for both datasets, not either, via a real, live deferral
against the real Airflow triggerer; it calls ``csv_processor.engine.process()``
directly to land real ``ingestion_metadata``/``customers_valid``/
``orders_valid`` rows for each dataset in turn.

Requires the full docker-compose stack (``make up``) already running and
healthy (this file's own ``<precondition>``), matching every other file
under ``tests/e2e/``.

``generator/generate_csv.py`` has no ``__init__.py`` and is not an installed
package, so it is loaded via ``importlib.util.spec_from_file_location`` --
the same convention already established by ``tests/e2e/test_csv_ingest_e2e.py``
and ``tests/e2e/test_correlated_report_e2e.py``, never a plain ``import``.
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

from scripts import dag_polling

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"

_GENERATE_CSV_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_GENERATE_CSV_SPEC = importlib.util.spec_from_file_location("generate_csv", _GENERATE_CSV_PATH)
assert _GENERATE_CSV_SPEC is not None and _GENERATE_CSV_SPEC.loader is not None
generate_csv = importlib.util.module_from_spec(_GENERATE_CSV_SPEC)
# Register in sys.modules BEFORE exec_module: generate_csv.py's frozen
# dataclasses (GeneratedCsv/CorrelatedDatasets) use postponed annotations,
# and dataclasses' forward-ref resolution looks the module up via
# sys.modules[cls.__module__] -- mirrors test_correlated_report_e2e.py's own
# documented workaround.
sys.modules["generate_csv"] = generate_csv
_GENERATE_CSV_SPEC.loader.exec_module(generate_csv)

_DAG_ID = "customers_orders_report"
_SENSOR_TASK_ID = "wait_for_both_datasets"
_REPORT_TASK_ID = "build_report_task"


@pytest.fixture(autouse=True)
def clean_customers_orders_report_partition(oracle_cursor: oracledb.Cursor) -> None:
    """Delete today's ``ingestion_metadata`` rows for ``customers``/``orders``
    BEFORE the test -- a clean slate is required to prove genuine deferral
    (mirrors this project's established Pitfall-4 discipline against
    stale-state false starts: a leftover row from an earlier run today would
    let the sensor find both datasets already present and skip deferring
    entirely)."""
    oracle_cursor.execute(
        "DELETE FROM ingestion_metadata WHERE dataset IN ('customers', 'orders') "
        "AND TRUNC(processed_at) = TRUNC(SYSDATE)"
    )
    oracle_cursor.connection.commit()


def test_report_ready_dag_defers_then_fires_once_both_datasets_present() -> None:
    jwt_token = dag_polling.get_jwt_token(dag_polling.AIRFLOW_BASE_URL)

    # (1) Trigger customers_orders_report -- no runtime conf needed (dataset-agnostic).
    run_id = dag_polling.trigger_dag_generic(_DAG_ID)

    # (2) The sensor must reach "deferred" BEFORE any ingestion happens.
    dag_polling.wait_for_task_state(
        dag_polling.AIRFLOW_BASE_URL,
        run_id,
        _SENSOR_TASK_ID,
        jwt_token,
        "deferred",
        timeout=180.0,
        dag_id=_DAG_ID,
    )

    # Generate both correlated CSVs up front (D-21/D-23's shared function --
    # orders.customer_id is a real, Zipf-weighted sample from customers' own
    # valid-row pool), but ingest them one at a time below so the sensor's
    # "waits for BOTH" behavior can be proven mid-flight.
    customers_config = load_config(
        _CONFIGS_DIR / "datasets" / "customers.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    orders_config = load_config(
        _CONFIGS_DIR / "datasets" / "orders.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    seed = time.time_ns() % (2**31)
    correlated = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=10,
        orders_rows=15,
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

    # (3) Ingest ONLY customers -- the sensor must remain deferred (proves it
    # waits for BOTH datasets, not either).
    customers_result = process(customers_path, customers_config)
    assert customers_result.status in {Status.SUCCESS, Status.SUCCESS_WITH_INVALID_ROWS}, (
        customers_result
    )

    state_after_one_dataset = dag_polling.poll_task_instance_state(
        dag_polling.AIRFLOW_BASE_URL, run_id, _SENSOR_TASK_ID, jwt_token, dag_id=_DAG_ID
    )
    assert state_after_one_dataset == "deferred", (
        f"sensor transitioned to {state_after_one_dataset!r} after only ONE dataset "
        "ingested -- expected it to still be waiting for the other"
    )

    # (4) Ingest orders too -- now both datasets have today's partition data.
    orders_result = process(orders_path, orders_config)
    assert orders_result.status in {Status.SUCCESS, Status.SUCCESS_WITH_INVALID_ROWS}, orders_result

    # (5) The sensor fires and build_report_task completes successfully.
    dag_polling.wait_for_task_state(
        dag_polling.AIRFLOW_BASE_URL,
        run_id,
        _REPORT_TASK_ID,
        jwt_token,
        "success",
        timeout=120.0,
        dag_id=_DAG_ID,
    )
