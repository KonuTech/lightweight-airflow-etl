"""The hourly ``csv_generate_schedule`` orchestrator DAG (SCHED-01, SCHED-03,
SCHED-04, SCHED-05, SCHED-06, SCHED-08, SCHED-10; D-01 through D-19).

Generates a fresh, correlated ``customers``+``orders`` CSV pair each hour
(D-03/D-04/D-05), then sequentially chain-triggers the existing, unmodified
``csv_ingest``/``report_ready`` DAGs (SCHED-03/SCHED-06, D-06/D-07/D-08),
logs a one-line cascade summary (SCHED-07, D-12/D-13/D-14), and best-effort
cleans up CSVs older than 30 days (SCHED-10, D-16/D-17/D-18) -- all without
ever touching ``csv_ingest.py``/``report_ready.py`` themselves.

Task graph: ``generate_task -> trigger_customers -> trigger_orders ->
trigger_report_ready -> summary_task -> retention_task``. ``trigger_orders``
must run strictly after ``trigger_customers`` fully commits (SCHED-03) --
Phase 7's DB-level ``BEFORE INSERT`` trigger on ``orders_valid`` rejects any
row whose ``customer_id`` doesn't already exist in ``customers_valid``.

D-07 (corrected): all three ``TriggerDagRunOperator`` tasks set
``skip_when_already_exists=True`` *and* ``reset_dag_run=True``. Traced
against the installed ``apache-airflow-providers-standard==1.17.0`` source,
``skip_when_already_exists`` alone is state-blind -- it only checks whether a
DagRun for the deterministic ``trigger_run_id`` *exists*, not whether it
*succeeded*, so a manual retry after a genuinely FAILED child run would
silently resolve to ``skipped`` (masking the failure as a cascading
"success") without ``reset_dag_run=True``. Setting ``reset_dag_run=True``
makes a retry actually clear and re-run the existing child DagRun -- a safe
no-op for an already-succeeded run (this project's checksum-keyed
idempotency guarantees re-ingesting the same data is harmless) and a
correct re-attempt for a failed one.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import UTC, datetime, timedelta

from _common import paths
from _common.generate_schedule_helpers import (
    derive_seed,
    format_cascade_summary,
    retention_sweep,
)
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from csv_processor import load

# D-12/D-13: bind-parameterized -- ``dataset`` is never string-interpolated
# into the SQL text itself (Tampering mitigation, T-9-01 in this plan's
# threat model).
_LATEST_INGESTION_SQL = """
SELECT total_rows, valid_rows, invalid_rows
FROM ingestion_metadata
WHERE dataset = :dataset
ORDER BY processed_at DESC
FETCH FIRST 1 ROW ONLY
"""

# D-16: retention_task's cutoff window.
_RETENTION_DAYS = 30


@dag(
    dag_id="csv_generate_schedule",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    params={
        "rows": Param(100, type="integer", minimum=1),
        "invalid_ratio": Param(0.1, type="number", minimum=0.0, maximum=1.0),
    },
)
def csv_generate_schedule() -> None:
    @task(retries=0)
    def generate_task() -> None:
        """Generate a fresh, correlated customers+orders CSV pair (D-03/D-05).

        Derives its seed from this DagRun's own ``logical_date`` via
        ``derive_seed()`` (D-04) -- the same ``logical_date`` always
        produces the same seed, so a task retry regenerates identical data.
        """
        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        # Airflow 3.x's ``logical_date`` is nullable for manually/API-triggered
        # runs (confirmed live against this project's pinned 3.3.1 -- a run
        # triggered with the documented ``{"logical_date": null}`` body has a
        # genuine ``None`` here, not an auto-assigned "now"). ``run_after`` is
        # the one timestamp Airflow 3.x guarantees non-null on every DagRun
        # (``DagRunProtocol.run_after: AwareDatetime``), so it's the correct
        # fallback -- retry-reproducibility (D-04) still holds for scheduled
        # runs, which always carry a real ``logical_date``.
        seed = derive_seed(dag_run.logical_date or dag_run.run_after)
        rows = ctx["params"]["rows"]
        invalid_ratio = ctx["params"]["invalid_ratio"]

        subprocess.run(
            [
                sys.executable,
                "/opt/airflow/generator/generate_csv.py",
                "--correlated",
                "--rows",
                str(rows),
                "--invalid-ratio",
                str(invalid_ratio),
                "--seed",
                str(seed),
                "--compress",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    trigger_customers = TriggerDagRunOperator(
        task_id="trigger_customers",
        trigger_dag_id="csv_ingest",
        conf={"dataset": "customers", "config_path": "configs/datasets/customers.json"},
        trigger_run_id="{{ dag_run.run_id }}__customers",
        skip_when_already_exists=True,
        reset_dag_run=True,
        wait_for_completion=True,
        deferrable=True,
        fail_when_dag_is_paused=True,
        poke_interval=10,
        retries=0,
    )

    trigger_orders = TriggerDagRunOperator(
        task_id="trigger_orders",
        trigger_dag_id="csv_ingest",
        conf={"dataset": "orders", "config_path": "configs/datasets/orders.json"},
        trigger_run_id="{{ dag_run.run_id }}__orders",
        skip_when_already_exists=True,
        reset_dag_run=True,
        wait_for_completion=True,
        deferrable=True,
        fail_when_dag_is_paused=True,
        poke_interval=10,
        retries=0,
    )

    trigger_report_ready = TriggerDagRunOperator(
        task_id="trigger_report_ready",
        trigger_dag_id="report_ready",
        trigger_run_id="{{ dag_run.run_id }}__report_ready",
        skip_when_already_exists=True,
        reset_dag_run=True,
        wait_for_completion=True,
        deferrable=True,
        fail_when_dag_is_paused=True,
        poke_interval=10,
        retries=0,
    )

    @task(trigger_rule="none_failed_min_one_success", retries=0)
    def summary_task() -> None:
        """Log one cascade summary line built from ``format_cascade_summary()``
        (SCHED-07, D-12/D-13/D-14)."""
        connection = load.get_connection()
        try:
            cursor = connection.cursor()
            dataset_results: dict[str, dict[str, int] | None] = {}
            for dataset in ("customers", "orders"):
                cursor.execute(_LATEST_INGESTION_SQL, dataset=dataset)
                row = cursor.fetchone()
                dataset_results[dataset] = (
                    None
                    if row is None
                    else {"total_rows": row[0], "valid_rows": row[1], "invalid_rows": row[2]}
                )
        finally:
            connection.close()
        logging.getLogger("airflow.task").info(format_cascade_summary(dataset_results))

    @task(trigger_rule="none_failed_min_one_success", retries=0)
    def retention_task() -> None:
        """Best-effort delete CSVs older than 30 days (SCHED-10, D-16/D-17/D-18).

        ``retention_sweep()`` itself never raises (implemented in Plan
        09-01) -- no ``try/except`` needed in this task body.
        """
        logger = logging.getLogger("airflow.task")
        cutoff = datetime.now(UTC) - timedelta(days=_RETENTION_DAYS)
        for dataset in ("customers", "orders"):
            deleted, skipped = retention_sweep(paths.DATA_ROOT / dataset, dataset, cutoff)
            for path in deleted:
                logger.info("retention: deleted %s (older than %d days)", path, _RETENTION_DAYS)
            for path, reason in skipped:
                logger.warning("retention: skipped %s (%s)", path, reason)

    generate_task() >> trigger_customers >> trigger_orders >> trigger_report_ready
    trigger_report_ready >> summary_task() >> retention_task()


csv_generate_schedule()
