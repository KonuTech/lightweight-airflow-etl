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
"""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta

from _common.generate_schedule_helpers import derive_seed
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task


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
        logical_date = ctx["dag_run"].logical_date
        seed = derive_seed(logical_date)
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
        wait_for_completion=True,
        deferrable=True,
        fail_when_dag_is_paused=True,
        poke_interval=10,
        retries=0,
    )

    generate_task() >> trigger_customers >> trigger_orders >> trigger_report_ready
