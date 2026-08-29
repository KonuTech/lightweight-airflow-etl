"""Tests for ``_common.reporting.format_summary_log`` (DAG-04).

Proves the exact field list/format ``report_result_task`` logs.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.reporting import format_summary_log  # noqa: E402


def test_format_summary_log_contains_all_required_fields() -> None:
    result = {
        "status": "SUCCESS",
        "dataset": "customers",
        "file_name": "customers_20260829.csv",
        "total_rows": 10,
        "valid_rows": 9,
        "invalid_rows": 1,
        "duration_seconds": 1.234,
        "checksum": "abc",
    }

    line = format_summary_log(result)

    assert "dataset=customers" in line
    assert "file=customers_20260829.csv" in line
    assert "status=SUCCESS" in line
    assert "total=10" in line
    assert "valid=9" in line
    assert "invalid=1" in line
    assert "duration=1.23s" in line
