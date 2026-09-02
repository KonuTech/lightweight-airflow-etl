"""Tests for ``_common.generate_schedule_helpers`` (SCHED-02, SCHED-07,
SCHED-10) -- proves ``derive_seed()``, ``format_cascade_summary()``, and
``retention_sweep()`` in isolation, without a live Airflow context or Oracle
connection.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.generate_schedule_helpers import (  # noqa: E402
    derive_seed,
    format_cascade_summary,
    retention_sweep,
)


def test_derive_seed_matches_documented_format() -> None:
    logical_date = datetime(2026, 9, 1, 14, 5, tzinfo=UTC)

    assert derive_seed(logical_date) == 202609011405


def test_seed_varies_by_hour() -> None:
    hour_14 = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)
    hour_15 = datetime(2026, 9, 1, 15, 0, tzinfo=UTC)

    assert derive_seed(hour_14) != derive_seed(hour_15)
    # Same logical_date reproduces the same seed on retry (D-04).
    assert derive_seed(hour_14) == derive_seed(hour_14)


def test_seed_varies_within_the_same_hour_at_five_minute_cadence() -> None:
    """Regression test for the MVP's 5-minute schedule: hour-only seed
    granularity would give every run within the same hour an identical
    seed/checksum, silently no-op-ing 11 of every 12 runs via
    checksum-keyed idempotency (Phase 9's "Pitfall 1", reborn)."""
    minute_00 = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)
    minute_05 = datetime(2026, 9, 1, 14, 5, tzinfo=UTC)

    assert derive_seed(minute_00) != derive_seed(minute_05)


def test_summary_format() -> None:
    dataset_results = {
        "customers": {"total_rows": 100, "valid_rows": 95, "invalid_rows": 5},
        "orders": {"total_rows": 50, "valid_rows": 48, "invalid_rows": 2},
    }

    line = format_cascade_summary(dataset_results)

    assert "customers=total:100,valid:95,invalid:5" in line
    assert "orders=total:50,valid:48,invalid:2" in line
    assert "customers_orders_report=OK" in line


def test_summary_format_handles_missing_dataset() -> None:
    dataset_results = {
        "customers": None,
        "orders": {"total_rows": 50, "valid_rows": 48, "invalid_rows": 2},
    }

    line = format_cascade_summary(dataset_results)

    assert "customers=NO_DATA" in line
    assert "orders=total:50,valid:48,invalid:2" in line
    assert "customers_orders_report=OK" in line


def test_retention_deletes_old_files(tmp_path: Path) -> None:
    cutoff = datetime(2026, 9, 1, tzinfo=UTC)
    old_date = (cutoff - timedelta(days=40)).strftime("%Y%m%d")
    recent_date = (cutoff + timedelta(days=5)).strftime("%Y%m%d")
    old_file = tmp_path / f"customers_{old_date}.csv"
    recent_file = tmp_path / f"customers_{recent_date}.csv.gz"
    old_file.write_text("old", encoding="utf-8")
    recent_file.write_text("recent", encoding="utf-8")

    deleted, skipped = retention_sweep(tmp_path, "customers", cutoff)

    assert old_file in deleted
    assert recent_file not in deleted
    assert skipped == []
    assert not old_file.exists()
    assert recent_file.exists()


def test_retention_skips_files_within_window(tmp_path: Path) -> None:
    cutoff = datetime(2026, 9, 1, tzinfo=UTC)
    recent_date = (cutoff + timedelta(days=5)).strftime("%Y%m%d")
    recent_file = tmp_path / f"customers_{recent_date}.csv"
    recent_file.write_text("recent", encoding="utf-8")

    deleted, skipped = retention_sweep(tmp_path, "customers", cutoff)

    assert deleted == []
    assert skipped == []
    assert recent_file.exists()


def test_retention_does_not_delete_non_canonical_backup_files(tmp_path: Path) -> None:
    """WR-03: a ``.bak``/``.orig``-suffixed file must never be deleted, even
    when it is old enough to otherwise qualify and its embedded date parses
    cleanly -- the glob must match only the exact
    ``<dataset>_<YYYYMMDD>.csv``/``.csv.gz`` shapes, not any filename that
    merely starts with that prefix.
    """
    cutoff = datetime(2026, 9, 1, tzinfo=UTC)
    old_date = (cutoff - timedelta(days=40)).strftime("%Y%m%d")
    backup_file = tmp_path / f"customers_{old_date}.csv.bak"
    orig_file = tmp_path / f"customers_{old_date}.csv.orig"
    backup_file.write_text("backup", encoding="utf-8")
    orig_file.write_text("orig", encoding="utf-8")

    deleted, skipped = retention_sweep(tmp_path, "customers", cutoff)

    assert deleted == []
    skipped_paths = [path for path, _reason in skipped]
    assert backup_file in skipped_paths
    assert orig_file in skipped_paths
    assert backup_file.exists()
    assert orig_file.exists()


def test_retention_never_raises(tmp_path: Path) -> None:
    cutoff = datetime(2026, 9, 1, tzinfo=UTC)
    old_date = (cutoff - timedelta(days=40)).strftime("%Y%m%d")

    unparseable_file = tmp_path / "customers_notadate.csv"
    unparseable_file.write_text("bad", encoding="utf-8")

    # A directory at the "old" dated path -- Path.unlink() raises
    # IsADirectoryError on it, never a plain file deletion.
    undeletable_dir = tmp_path / f"customers_{old_date}.csv"
    undeletable_dir.mkdir()

    deleted, skipped = retention_sweep(tmp_path, "customers", cutoff)

    assert deleted == []
    skipped_paths = [path for path, _reason in skipped]
    assert unparseable_file in skipped_paths
    assert undeletable_dir in skipped_paths
    assert undeletable_dir.exists()
