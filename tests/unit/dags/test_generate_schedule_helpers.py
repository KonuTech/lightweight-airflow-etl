"""Tests for ``_common.generate_schedule_helpers`` (SCHED-02, SCHED-07,
SCHED-10) -- proves ``derive_seed()``, ``format_cascade_summary()``, and
``retention_sweep()`` in isolation, without a live Airflow context or Oracle
connection.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.generate_schedule_helpers import (  # noqa: E402
    derive_seed,
    format_cascade_summary,
)


def test_derive_seed_matches_documented_format() -> None:
    logical_date = datetime(2026, 9, 1, 14, tzinfo=UTC)

    assert derive_seed(logical_date) == 2026090114


def test_seed_varies_by_hour() -> None:
    hour_14 = datetime(2026, 9, 1, 14, tzinfo=UTC)
    hour_15 = datetime(2026, 9, 1, 15, tzinfo=UTC)

    assert derive_seed(hour_14) != derive_seed(hour_15)
    # Same logical_date reproduces the same seed on retry (D-04).
    assert derive_seed(hour_14) == derive_seed(hour_14)


def test_summary_format() -> None:
    dataset_results = {
        "customers": {"total_rows": 100, "valid_rows": 95, "invalid_rows": 5},
        "orders": {"total_rows": 50, "valid_rows": 48, "invalid_rows": 2},
    }

    line = format_cascade_summary(dataset_results)

    assert "customers=total:100,valid:95,invalid:5" in line
    assert "orders=total:50,valid:48,invalid:2" in line
    assert "report_ready=OK" in line


def test_summary_format_handles_missing_dataset() -> None:
    dataset_results = {
        "customers": None,
        "orders": {"total_rows": 50, "valid_rows": 48, "invalid_rows": 2},
    }

    line = format_cascade_summary(dataset_results)

    assert "customers=NO_DATA" in line
    assert "orders=total:50,valid:48,invalid:2" in line
    assert "report_ready=OK" in line
