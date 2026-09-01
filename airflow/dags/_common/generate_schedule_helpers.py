"""``csv_generate_schedule``'s pure helper functions (SCHED-02, SCHED-07,
SCHED-10; D-04, D-12, D-13, D-14, D-16, D-18).

Plain, unit-testable functions with zero Airflow import -- mirrors
``_common/paths.py``'s and ``_common/reporting.py``'s established
zero-Airflow-import module convention. ``derive_seed()`` backs the
per-hour seed derivation (D-04); ``format_cascade_summary()`` backs the
cascade summary log line (SCHED-07, D-12/D-13/D-14); ``retention_sweep()``
backs the never-raising retention pass (SCHED-10, D-16/D-18).
"""

from __future__ import annotations

from datetime import datetime


def derive_seed(logical_date: datetime) -> int:
    """Derive a per-hour, retry-reproducible seed from ``logical_date`` (D-04).

    The same ``logical_date`` always produces the same seed (retry
    reproducibility); two different real hours always produce different
    seeds.

    Args:
        logical_date: The DagRun's ``logical_date`` (a plain, tz-aware
            ``datetime.datetime``, not ``pendulum.DateTime``).

    Returns:
        An integer seed in ``YYYYMMDDHH`` shape, e.g.
        ``datetime(2026, 9, 1, 14, tzinfo=UTC)`` -> ``2026090114``.
    """
    return int(logical_date.strftime("%Y%m%d%H"))


def format_cascade_summary(dataset_results: dict[str, dict[str, int] | None]) -> str:
    """Format the cascade's per-dataset row counts as one summary log line
    (SCHED-07, D-12/D-13/D-14).

    Args:
        dataset_results: A mapping of ``"customers"``/``"orders"`` to either
            a dict carrying ``total_rows``/``valid_rows``/``invalid_rows``,
            or ``None`` when that dataset has no ingestion data yet.

    Returns:
        A single line naming both datasets' total/valid/invalid row counts
        (or ``NO_DATA`` when a dataset's result is ``None``) plus a fixed
        ``report_ready=OK`` heartbeat token (D-14 -- never re-runs the
        business-report SQL here).
    """
    parts = []
    for dataset in ("customers", "orders"):
        result = dataset_results[dataset]
        if result is None:
            parts.append(f"{dataset}=NO_DATA")
        else:
            parts.append(
                f"{dataset}=total:{result['total_rows']},"
                f"valid:{result['valid_rows']},"
                f"invalid:{result['invalid_rows']}"
            )
    parts.append("report_ready=OK")
    return " ".join(parts)
