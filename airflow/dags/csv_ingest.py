"""The one, config-driven ``csv_ingest`` DAG (D-01/DAG-01/DAG-05).

Fully parameterized by runtime ``conf`` (``dataset`` name + ``config_path``) --
never one DAG per dataset. Delegates the entire detect->parse->validate->
normalize->chunk->load(Oracle) sequence to ``csv_processor.engine.process()``
(D-02/D-03/D-12) -- this file and ``_common/`` contain no CSV/Oracle logic of
their own, only thin wiring plus input-validation (T-05-01/T-05-02).

Task graph: ``load_config_task`` -> ``route_after_config`` -> either
``wait_for_file`` (deferrable ``FileSensor``, D-04) -> ``process_csv_task`` ->
``load_results_task`` -> ``report_result_task``, or straight to
``report_result_task`` on a CONFIGURATION_ERROR early exit (D-08).
"""

from __future__ import annotations

import logging

from _common import paths, reporting
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Param, dag, get_current_context, task
from csv_processor.config.errors import ConfigurationError
from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process
from csv_processor.models import ProcessingResult, Status


@dag(
    dag_id="csv_ingest",
    schedule=None,
    catchup=False,
    params={
        "dataset": Param("customers", type="string", enum=["customers", "orders"]),
        "config_path": Param("configs/datasets/customers.json", type="string"),
    },
)
def csv_ingest() -> None:
    @task
    def load_config_task() -> dict[str, object]:
        """Validate runtime conf and load the referenced dataset config.

        D-08: validates BOTH halves of runtime conf (dataset, config_path)
        before ever calling ``load_config``. On (ValueError,
        ConfigurationError) returns a CONFIGURATION_ERROR-shaped dict instead
        of letting the exception propagate (D-03/D-08's "domain failures
        don't fail the task").
        """
        ctx = get_current_context()
        dataset = ctx["params"]["dataset"]
        config_path = ctx["params"]["config_path"]

        try:
            paths.validate_dataset(dataset)
            resolved_path = paths.resolve_safe_config_path(config_path)
            config = load_config(resolved_path, defaults_path=paths.DEFAULTS_PATH)
        except (ValueError, ConfigurationError):
            return {
                "status": Status.CONFIGURATION_ERROR.value,
                "dataset": dataset,
                "file_name": "",
                "total_rows": 0,
                "valid_rows": 0,
                "invalid_rows": 0,
                "duration_seconds": 0.0,
                "checksum": None,
            }
        return config.model_dump(mode="json")

    config_dict = load_config_task()

    @task.branch
    def route_after_config(config_dict: dict[str, object]) -> str:
        """The one, deliberate, non-dataset-specific branch in this DAG (D-05).

        Keys off config validity only, never dataset identity.
        """
        if config_dict.get("status") == Status.CONFIGURATION_ERROR.value:
            return "report_result_task"
        return "wait_for_file"

    route = route_after_config(config_dict)

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath=(
            "/opt/airflow/data/{{ params.dataset }}/"
            "{{ ti.xcom_pull(task_ids='load_config_task')['file_pattern'] }}"
        ),
        deferrable=True,
        poke_interval=10,
        timeout=3600,
    )

    @task
    def process_csv_task(config_dict: dict[str, object]) -> dict[str, object]:
        """Call ``csv_processor.engine.process()`` exactly once -- the sole
        Oracle-writing integration point (D-02/D-12).

        Never reads ``wait_for_file.output`` (it's a bare bool,
        05-RESEARCH.md Pitfall 1) -- independently re-globs for the matched
        file instead.
        """
        ctx = get_current_context()
        dataset = ctx["params"]["dataset"]
        config = DatasetConfig.model_validate(config_dict)
        base_dir = paths.DATA_ROOT / dataset
        matched = paths.resolve_matched_file(base_dir, config.file_pattern)
        file_path = matched if matched is not None else base_dir / "__no_match_found__"
        # D-03/Pitfall 5: process() never raises for any of its 7 closed
        # Status values -- no try/except here that re-raises on any Status.
        result: ProcessingResult = process(file_path, config)
        return result.model_dump(mode="json")

    result_dict = process_csv_task(config_dict)

    @task
    def load_results_task(result_dict: dict[str, object]) -> dict[str, object]:
        """D-02: thin pass-through -- never imports csv_processor.load or
        oracledb, never calls process() again."""
        return result_dict

    final_result_dict = load_results_task(result_dict)

    @task(trigger_rule="none_failed_min_one_success")
    def report_result_task() -> None:
        """Log dataset/file/row counts/duration/status (DAG-04, D-07: logs
        only, no Slack/email)."""
        ctx = get_current_context()
        ti = ctx["ti"]
        outcome = ti.xcom_pull(task_ids="load_results_task") or ti.xcom_pull(
            task_ids="load_config_task"
        )
        logging.getLogger("airflow.task").info(reporting.format_summary_log(outcome))

    report = report_result_task()

    route >> wait_for_file >> result_dict
    route >> report
    final_result_dict >> report


csv_ingest()
