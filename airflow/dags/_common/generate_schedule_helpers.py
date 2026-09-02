"""``ingestion_cascade_orchestrator``'s pure helper functions (SCHED-02, SCHED-07,
SCHED-10; D-04, D-12, D-13, D-14, D-16, D-18).

Plain, unit-testable functions with zero Airflow import -- mirrors
``_common/paths.py``'s and ``_common/reporting.py``'s established
zero-Airflow-import module convention. ``derive_seed()`` backs the
per-hour seed derivation (D-04); ``format_cascade_summary()`` backs the
cascade summary log line (SCHED-07, D-12/D-13/D-14); ``retention_sweep()``
backs the never-raising retention pass (SCHED-10, D-16/D-18).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

# Matches exactly the two filename shapes generate_csv.py's output_path()
# produces after the "<dataset>_" prefix is stripped: <YYYYMMDD>.csv or
# <YYYYMMDD>.csv.gz. Anchored on both ends so a file such as
# "customers_20260101.csv.bak" (e.g. a manual operator backup) does NOT
# match, even though it starts with a valid <dataset>_<YYYYMMDD>.csv
# prefix -- WR-03: the previous glob-only match was broader than this
# function's own documented contract and risked deleting such files.
_FILENAME_RE = re.compile(r"^(\d{8})\.csv(\.gz)?$")


def derive_seed(logical_date: datetime) -> int:
    """Derive a per-run, retry-reproducible seed from ``logical_date`` (D-04).

    The same ``logical_date`` always produces the same seed (retry
    reproducibility); two different runs always produce different seeds.

    Minute granularity (not hour) -- at the MVP's 5-minute schedule cadence,
    hour-only granularity would give every run within the same hour an
    IDENTICAL seed, producing byte-identical CSVs whose checksum-keyed
    idempotency check (``csv_processor.engine.process()``) would silently
    no-op 11 of every 12 runs. This is Phase 9's own "Pitfall 1" bug,
    reborn at finer schedule granularity.

    Args:
        logical_date: The DagRun's ``logical_date`` (a plain, tz-aware
            ``datetime.datetime``, not ``pendulum.DateTime``).

    Returns:
        An integer seed in ``YYYYMMDDHHMM`` shape, e.g.
        ``datetime(2026, 9, 1, 14, 5, tzinfo=UTC)`` -> ``202609011405``.
    """
    return int(logical_date.strftime("%Y%m%d%H%M"))


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
        ``customers_orders_report=OK`` heartbeat token (D-14 -- never re-runs the
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
    parts.append("customers_orders_report=OK")
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

    for path in base_dir.glob(f"{dataset}_*"):
        match = _FILENAME_RE.match(path.name.removeprefix(f"{dataset}_"))
        if match is None:
            skipped.append((path, "does not match <dataset>_<YYYYMMDD>.csv[.gz]"))
            continue
        date_token = match.group(1)
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
