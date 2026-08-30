"""The ``report_ready`` DAG (D-26): senses when both ``customers`` and
``orders`` have ingested data for today's real wall-clock-date partition,
then builds/logs the same business-report SQL this project already uses
(never re-authored a fourth time).

Runs ALONGSIDE ``scripts/regenerate_readme_summary.py``'s CI-triggered path
(D-27) -- this is an additional, live in-Airflow path, not a replacement.

Task graph: ``wait_for_both_datasets`` (the deferrable ``ReportReadySensor``,
D-28) -> ``build_report_task`` (a thin worker task -- a normal blocking
``csv_processor.load.get_connection()`` here is correct, unlike the
sensor's own triggerer-side async polling, since this task runs on a normal
Airflow worker, never the shared triggerer event loop).
"""

from __future__ import annotations

import logging

from _common.oracle_partition_trigger import ReportReadySensor
from airflow.sdk import dag, task
from csv_processor import load

# Mirrored verbatim from scripts/regenerate_readme_summary.py's own
# _BUSINESS_REPORT_SQL (itself already mirrored from
# scripts/verify_evidence.sql) -- never re-authored a fourth time, per this
# project's established "never re-author the same SQL twice" discipline.
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
FETCH FIRST 10 ROWS ONLY
"""


@dag(
    dag_id="report_ready",
    schedule=None,
    catchup=False,
)
def report_ready() -> None:
    sensor = ReportReadySensor(task_id="wait_for_both_datasets")

    @task
    def build_report_task() -> None:
        """Query and log the business report (D-27: logs only, matching
        ``csv_ingest.py``'s ``report_result_task`` shape -- no Slack/email)."""
        connection = load.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(_BUSINESS_REPORT_SQL)
            rows = cursor.fetchall()
        finally:
            connection.close()
        logging.getLogger("airflow.task").info(
            "report_ready business report (%d rows): %s", len(rows), rows
        )

    sensor >> build_report_task()


report_ready()
