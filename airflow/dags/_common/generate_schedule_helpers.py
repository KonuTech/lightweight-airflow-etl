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

from datetime import UTC, datetime
from pathlib import Path


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


def retention_sweep(
    base_dir: Path, dataset: str, cutoff: datetime
) -> tuple[list[Path], list[tuple[Path, str]]]:
    """Delete ``dataset``'s CSV/CSV.GZ files under ``base_dir`` older than
    ``cutoff``, without ever raising (SCHED-10, D-16/D-18).

    Matches the ``<dataset>_<YYYYMMDD>.csv``/``<dataset>_<YYYYMMDD>.csv.gz``
    filename convention established by ``generator/generate_csv.py``'s
    ``output_path()``/``write_staged()``.

    Args:
        base_dir: The dataset's data directory (e.g.
            ``/opt/airflow/data/customers``).
        dataset: The dataset name, used both for the glob pattern and to
            strip the filename prefix when parsing the embedded date.
        cutoff: Files whose embedded date is strictly older than this are
            deleted; files on or after it are left untouched.

    Returns:
        A ``(deleted, skipped)`` tuple: ``deleted`` is the list of paths
        successfully removed; ``skipped`` is a list of
        ``(path, reason)`` pairs for entries whose date token couldn't be
        parsed or whose deletion failed -- never raised, only recorded.
    """
    deleted: list[Path] = []
    skipped: list[tuple[Path, str]] = []

    for path in base_dir.glob(f"{dataset}_*.csv*"):
        date_token = path.name.removeprefix(f"{dataset}_").split(".", 1)[0]
        try:
            file_date = datetime.strptime(date_token, "%Y%m%d").replace(tzinfo=UTC)
        except ValueError as exc:
            skipped.append((path, f"unparseable filename: {exc}"))
            continue

        if file_date >= cutoff:
            continue

        try:
            path.unlink()
        except OSError as exc:
            skipped.append((path, f"delete failed: {exc}"))
            continue

        deleted.append(path)

    return deleted, skipped
