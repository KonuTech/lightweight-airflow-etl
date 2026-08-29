"""Shared fixtures for ``tests/unit/dags/`` -- Phase 5's DAG-side pure-Python
helper tests (05-01-PLAN.md Task 2).

``_common`` (``airflow/dags/_common/``) is only importable from within the
DAG folder at Airflow runtime, not via the project's normal ``pythonpath``
(pyproject.toml's ``[tool.pytest.ini_options] pythonpath = ["."]`` points at
the repo root, not ``airflow/dags/``) -- each test module that imports from
``_common`` inserts ``airflow/dags`` onto ``sys.path`` itself before doing so.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from csv_processor.config.loader import load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"


@pytest.fixture
def dataset_configs():
    """Both real, validated dataset configs -- reused across the three test
    files in this directory to prove DAG-05's "same helper, both datasets,
    zero branching" without re-loading the configs in every test.
    """
    return {
        "customers": load_config(
            _CONFIGS_DIR / "datasets" / "customers.json", defaults_path=_DEFAULTS_PATH
        ),
        "orders": load_config(
            _CONFIGS_DIR / "datasets" / "orders.json", defaults_path=_DEFAULTS_PATH
        ),
    }
