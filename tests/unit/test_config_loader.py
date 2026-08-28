"""Tests for csv_processor.config.loader.load_config() and the config model tree.

Covers CONFIG-01/CONFIG-02 end-to-end for the ``customers`` dataset (02-01-PLAN.md
Task 1's <behavior> block): successful load+merge, frozen/extra-forbid enforcement,
empty-columns rejection, multi-field error aggregation, missing/empty-file handling,
and the delimiter/decimal-separator collision validator (D-17 gap resolution).

Uses plain pytest functions (Claude's discretion per 02-CONTEXT.md) rather than
stdlib unittest.TestCase, since this is the project's first pytest-based test file
(Wave 0 gap closure) and pytest's fixture/parametrize idioms fit the many small
independent cases below better than a TestCase class would.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from csv_processor.config import ConfigurationError, DatasetConfig, load_config

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_ORDERS_PATH = _CONFIGS_DIR / "datasets" / "orders.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"

_MINIMAL_VALID_DATASET: dict = {
    "dataset": "x",
    "file_pattern": "x_*.csv",
    "columns": [
        {"name": "a", "type": "string", "nullable": False, "required": True},
    ],
    "oracle": {"valid_table": "x_valid", "invalid_table": "x_invalid"},
    "processing": {"chunk_size": 1},
}


def test_load_config_returns_validated_customers_dataset() -> None:
    config = load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)

    assert config.dataset == "customers"
    assert len(config.columns) == 6
    assert config.oracle.valid_table == "customers_valid"
    assert config.oracle.invalid_table == "customers_invalid"


def test_load_config_returns_validated_orders_dataset() -> None:
    """02-02 Task 1: proves the second dataset (`orders`) validates through the
    identical, unmodified load_config() path Plan 01 proved for `customers`
    (CONFIG-01/GEN-01's "both datasets" requirement)."""
    config = load_config(_ORDERS_PATH, defaults_path=_DEFAULTS_PATH)

    assert config.dataset == "orders"
    assert len(config.columns) == 4
    assert config.oracle.valid_table == "orders_valid"
    assert config.oracle.invalid_table == "orders_invalid"

    amount_column = next(column for column in config.columns if column.name == "amount")
    assert amount_column.type == "decimal"
    assert amount_column.precision == 12
    assert amount_column.scale == 2


def test_dataset_config_is_frozen() -> None:
    config = load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)

    with pytest.raises(ValidationError):
        config.dataset = "mutated"  # type: ignore[misc]


def test_empty_columns_list_rejected() -> None:
    bad = {**_MINIMAL_VALID_DATASET, "columns": []}

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)


def test_load_config_aggregates_multiple_field_errors_in_one_pass(tmp_path: Path) -> None:
    bad_dataset = {
        # "dataset" missing entirely
        "file_pattern": "x_*.csv",
        "columns": [
            {"name": "a", "type": "not-a-real-type", "nullable": False, "required": True},
        ],
        "oracle": {"valid_table": "x_valid", "invalid_table": "x_invalid"},
        "processing": {"chunk_size": 0},
    }
    dataset_path = tmp_path / "bad.json"
    dataset_path.write_text(json.dumps(bad_dataset), encoding="utf-8")
    defaults_path = tmp_path / "defaults.json"
    defaults_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ConfigurationError) as excinfo:
        load_config(dataset_path, defaults_path=defaults_path)

    errors = excinfo.value.context["errors"]
    error_locs = {tuple(e["loc"]) for e in errors}
    assert ("dataset",) in error_locs
    assert ("columns", 0, "type") in error_locs
    assert ("processing", "chunk_size") in error_locs
    assert len(errors) >= 3


def test_load_config_missing_file_raises_configuration_error(tmp_path: Path) -> None:
    missing_path = tmp_path / "does_not_exist.json"
    defaults_path = tmp_path / "defaults.json"
    defaults_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_config(missing_path, defaults_path=defaults_path)


def test_load_config_empty_file_raises_configuration_error(tmp_path: Path) -> None:
    empty_path = tmp_path / "empty.json"
    empty_path.write_text("", encoding="utf-8")
    defaults_path = tmp_path / "defaults.json"
    defaults_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_config(empty_path, defaults_path=defaults_path)


def test_delimiter_decimal_separator_collision_rejected() -> None:
    bad = {
        **_MINIMAL_VALID_DATASET,
        "csv": {"delimiter": ",", "decimal_separator": ","},
    }

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)


def test_unrecognized_top_level_key_rejected() -> None:
    bad = {**_MINIMAL_VALID_DATASET, "not_a_real_field": True}

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)


def test_unrecognized_nested_key_rejected() -> None:
    bad = {
        **_MINIMAL_VALID_DATASET,
        "oracle": {
            "valid_table": "x_valid",
            "invalid_table": "x_invalid",
            "connection_string": "should-never-exist",
        },
    }

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)


def test_date_type_without_format_rejected() -> None:
    bad = {
        **_MINIMAL_VALID_DATASET,
        "columns": [
            {"name": "d", "type": "date", "nullable": True, "required": True},
        ],
    }

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)


def test_decimal_type_without_precision_scale_rejected() -> None:
    bad = {
        **_MINIMAL_VALID_DATASET,
        "columns": [
            {"name": "amt", "type": "decimal", "nullable": True, "required": True},
        ],
    }

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)


def test_decimal_scale_greater_than_precision_rejected() -> None:
    bad = {
        **_MINIMAL_VALID_DATASET,
        "columns": [
            {
                "name": "amt",
                "type": "decimal",
                "nullable": True,
                "required": True,
                "precision": 2,
                "scale": 4,
            },
        ],
    }

    with pytest.raises(ValidationError):
        DatasetConfig.model_validate(bad)
