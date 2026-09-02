"""Automated end-to-end proof for TEST-03/D-08: an HTTP request triggers the
real ``csv_to_oracle_ingest`` DAG, the deferred ``wait_for_file`` sensor genuinely
wakes (not polls) once a fixture file appears, ``process_csv_task`` runs the
real CSV engine, and correct/incorrect rows land in Oracle's
``customers_valid``/``customers_invalid`` tables -- asserted via a real
``oracledb`` ``SELECT``, never ``DagRun.state``/``result["status"]`` alone
(D-08).

This is Phase 6's tracer (06-01-PLAN.md) -- production-quality, the
permanent e2e regression test for TEST-03, not a throwaway prototype.
Requires the full docker-compose stack (``make up``) already running and
healthy (this file's own ``<precondition>``).

``scripts/dag_polling.py`` and ``generator/generate_csv.py`` have no
``__init__.py`` and are not installed packages, so both are loaded via
``importlib.util.spec_from_file_location`` -- the same convention already
established by ``tests/test_verify_environment.py`` and
``tests/unit/test_generate_csv.py``, never a plain ``import``.
"""

from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import oracledb
from csv_processor.config.loader import load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_CUSTOMERS_DATA_DIR = _REPO_ROOT / "data" / "customers"
_CUSTOMERS_CONFIG_PATH = "configs/datasets/customers.json"

_DAG_POLLING_PATH = _REPO_ROOT / "scripts" / "dag_polling.py"
_DAG_POLLING_SPEC = importlib.util.spec_from_file_location("dag_polling", _DAG_POLLING_PATH)
assert _DAG_POLLING_SPEC is not None and _DAG_POLLING_SPEC.loader is not None
dag_polling = importlib.util.module_from_spec(_DAG_POLLING_SPEC)
_DAG_POLLING_SPEC.loader.exec_module(dag_polling)

_GENERATE_CSV_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_GENERATE_CSV_SPEC = importlib.util.spec_from_file_location("generate_csv", _GENERATE_CSV_PATH)
assert _GENERATE_CSV_SPEC is not None and _GENERATE_CSV_SPEC.loader is not None
generate_csv = importlib.util.module_from_spec(_GENERATE_CSV_SPEC)
# Register in sys.modules BEFORE exec_module: generate_csv.py's frozen
# dataclass (GeneratedCsv) uses postponed annotations, and dataclasses'
# forward-ref resolution looks the module up via sys.modules[cls.__module__]
# -- without this it raises AttributeError on a None module during class
# creation (mirrors tests/unit/test_generate_csv.py's own workaround).
sys.modules["generate_csv"] = generate_csv
_GENERATE_CSV_SPEC.loader.exec_module(generate_csv)


def _clear_existing_customers_fixtures() -> None:
    """Delete every pre-existing file matching ``customers_*.csv*`` on the
    HOST data dir BEFORE triggering.

    The DAG's ``wait_for_file`` sensor glob-matches this exact pattern
    (``airflow/dags/csv_to_oracle_ingest.py``'s ``FileSensor``), so a stale fixture
    left over from a prior manual/live-verification run (e.g. Phase 5's own
    ``docs/airflow-dag.md`` evidence capture, or a prior local ``make
    generate``) would make the sensor match immediately and never defer,
    silently invalidating this test's central deferred-wake assertion. Not
    in the plan's literal action text -- a Rule 1 fix required for the
    "confirmed ABSENT" precondition Pitfall 4 demands, since the sensor's
    glob is broader than any single run-unique filename this test controls.
    """
    _CUSTOMERS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for stale in _CUSTOMERS_DATA_DIR.glob("customers_*.csv*"):
        stale.unlink()


def test_wait_for_file_defers_before_file_exists_then_lands_in_oracle(
    oracle_cursor: oracledb.Cursor,
) -> None:
    _clear_existing_customers_fixtures()

    # Step 1: trigger with the target fixture file confirmed ABSENT.
    run_id = dag_polling.trigger_dag("customers", _CUSTOMERS_CONFIG_PATH)
    jwt_token = dag_polling.get_jwt_token(dag_polling.AIRFLOW_BASE_URL)

    # Step 2: poll until wait_for_file genuinely reaches "deferred" BEFORE
    # any fixture file is written -- Pitfall 4's exact ordering. Never write
    # the file before this call returns.
    #
    # timeout=180 (not the 60s default): on a genuinely cold docker-compose
    # stack (CI, first boot), `docker compose up --wait`'s healthchecks only
    # confirm the scheduler process is alive, not that it has completed its
    # first DAG-parse-and-schedule cycle for this specific triggered run.
    # Confirmed live: PR #1's oracle-e2e run timed out at the 60s default
    # with last observed state 'None' (no task instance yet) on a fresh
    # runner; the identical test passes in ~seconds against this project's
    # own warm local dev stack, where the DAG is already parsed and cached.
    # This does not weaken the assertion -- it must still reach exactly
    # "deferred", just with headroom for cold-start scheduling latency.
    dag_polling.wait_for_task_state(
        dag_polling.AIRFLOW_BASE_URL, run_id, "wait_for_file", jwt_token, "deferred", timeout=180.0
    )

    # Step 3: only THEN write a freshly-generated ~20-row customers CSV
    # (invalid_ratio ~0.2 so both valid and invalid rows exist), with a
    # run-unique (microsecond) filename so LOAD-04's checksum-based
    # idempotency never collides with a prior test run.
    config = load_config(
        _CONFIGS_DIR / "datasets" / "customers.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    unique_suffix = time.time_ns()
    generated = generate_csv.generate_rows(
        config, rows=20, invalid_ratio=0.2, seed=unique_suffix % (2**31)
    )
    file_name = f"customers_{unique_suffix}.csv"
    fixture_path = _CUSTOMERS_DATA_DIR / file_name
    generate_csv.write_csv(generated, config, fixture_path)

    expected_valid_customer_ids = [
        row[0]
        for row, category in zip(generated.rows, generated.categories, strict=True)
        if category is None
    ]
    expected_invalid_count = sum(1 for category in generated.categories if category is not None)
    assert expected_valid_customer_ids, "fixture generated zero valid rows -- test is meaningless"
    assert expected_invalid_count > 0, "fixture generated zero invalid rows -- test is meaningless"

    # Step 4: poll the DAG run to completion.
    result = dag_polling.wait_for_dag_run_result(dag_polling.AIRFLOW_BASE_URL, run_id, jwt_token)
    assert result["status"] in {"SUCCESS", "SUCCESS_WITH_INVALID_ROWS"}, result

    # Step 5: D-08's literal proof -- real Oracle rows, never DagRun.state/
    # result["status"] alone.
    placeholders = ", ".join(f":{i}" for i in range(len(expected_valid_customer_ids)))
    oracle_cursor.execute(
        f"SELECT COUNT(*) FROM customers_valid WHERE customer_id IN ({placeholders})",
        expected_valid_customer_ids,
    )
    (valid_count,) = oracle_cursor.fetchone()
    assert valid_count == len(expected_valid_customer_ids)

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM customers_invalid WHERE source_file = :source_file",
        {"source_file": file_name},
    )
    (invalid_count,) = oracle_cursor.fetchone()
    assert invalid_count == expected_invalid_count
