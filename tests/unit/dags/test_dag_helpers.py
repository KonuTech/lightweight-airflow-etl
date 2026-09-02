"""Tests for ``_common.paths.resolve_matched_file`` (DAG-05, Pitfall 1's fix).

Proves the most-recent-match/no-match behavior and that the SAME helper
works for both datasets' real ``file_pattern`` values with zero
dataset-specific branching, in the test or in the helper itself.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.paths import resolve_matched_file  # noqa: E402


def test_resolve_matched_file_returns_most_recent_match(tmp_path: Path) -> None:
    """Regression test: a stale older-dated file must never win over a
    newer one -- picking the oldest match silently kept re-ingesting a
    stale file forever in real hourly operation (live-reproduced bug)."""
    (tmp_path / "customers_20260101.csv").write_text("a", encoding="utf-8")
    (tmp_path / "customers_20260201.csv").write_text("b", encoding="utf-8")

    result = resolve_matched_file(tmp_path, "customers_*.csv*")

    assert result is not None
    assert result.name == "customers_20260201.csv"


def test_resolve_matched_file_returns_none_when_no_match(tmp_path: Path) -> None:
    assert resolve_matched_file(tmp_path, "customers_*.csv*") is None


@pytest.mark.parametrize(
    ("dataset", "filename"),
    [
        ("customers", "customers_20990101.csv"),
        ("orders", "orders_20990101.csv.gz"),
    ],
)
def test_resolve_matched_file_works_for_both_dataset_patterns(
    tmp_path: Path, dataset_configs, dataset: str, filename: str
) -> None:
    (tmp_path / filename).write_text("data", encoding="utf-8")

    result = resolve_matched_file(tmp_path, dataset_configs[dataset].file_pattern)

    assert result is not None
    assert result.name == filename
