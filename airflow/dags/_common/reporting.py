"""``report_result_task``'s log-line formatter (DAG-04, D-07).

Plain, unit-testable function with zero Airflow import -- ``report_result_task``
itself just calls this and logs the result (Airflow task logging only, no
external notification, per D-07).
"""

from __future__ import annotations


def format_summary_log(result: dict) -> str:
    """Format ``result`` (a ``ProcessingResult``-shaped dict) as one summary log line.

    Args:
        result: A dict carrying at least ``dataset``, ``file_name``,
            ``status``, ``total_rows``, ``valid_rows``, ``invalid_rows``,
            ``duration_seconds`` -- exactly ``ProcessingResult``'s field
            names (``packages/csv-processor/src/csv_processor/models.py``),
            matched by both the success path (``load_results_task``'s XCom)
            and the config-error early-exit path (``load_config_task``'s
            own CONFIGURATION_ERROR-shaped dict).

    Returns:
        A single line: ``"dataset=... file=... status=... total=... valid=...
        invalid=... duration=...s"``.
    """
    return (
        f"dataset={result['dataset']} "
        f"file={result['file_name']} "
        f"status={result['status']} "
        f"total={result['total_rows']} "
        f"valid={result['valid_rows']} "
        f"invalid={result['invalid_rows']} "
        f"duration={result['duration_seconds']:.2f}s"
    )
