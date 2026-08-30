"""Tests for ``_common.paths.validate_dataset`` and
``_common.paths.resolve_safe_config_path`` (DAG-02, T-05-01/T-05-02).

The absolute-path rejection case (``test_resolve_safe_config_path_rejects_absolute_paths``)
is the single most important negative case in this file -- it proves the
``Path.__truediv__``/``os.path.join`` silent-base-discard bypass (T-05-01) is
closed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.paths import resolve_safe_config_path, validate_dataset  # noqa: E402


def test_validate_dataset_accepts_known_datasets() -> None:
    validate_dataset("customers")
    validate_dataset("orders")


def test_validate_dataset_rejects_unknown_dataset() -> None:
    with pytest.raises(ValueError):
        validate_dataset("../../etc")


@pytest.mark.parametrize(
    "config_path",
    ["configs/datasets/customers.json", "configs/datasets/orders.json"],
)
def test_resolve_safe_config_path_accepts_real_dataset_configs(config_path: str) -> None:
    result = resolve_safe_config_path(config_path)

    expected_name = Path(config_path).name
    # Adjusted for the /opt/airflow prefix the function assumes -- this test
    # runs on the host, not inside the container, so assert on the trailing
    # parts rather than a literal /opt/airflow-prefixed equality.
    assert result.parts[-3:] == ("configs", "datasets", expected_name)


def test_resolve_safe_config_path_rejects_traversal() -> None:
    with pytest.raises(ValueError):
        resolve_safe_config_path("../../etc/passwd")


def test_resolve_safe_config_path_rejects_absolute_paths() -> None:
    """T-05-01's mitigation: an absolute config_path must never bypass the
    allowlist via Path.__truediv__'s silent base-discard behavior."""
    with pytest.raises(ValueError):
        resolve_safe_config_path("/etc/passwd")
