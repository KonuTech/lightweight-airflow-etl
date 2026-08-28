"""Tests for generator/generate_csv.py (GEN-01), scoped to `--dataset customers`
(Plan 02 extends coverage to `orders`).

`generator/` has no `__init__.py` and is not an installed package (matches this
project's own `tests/test_verify_environment.py` convention for `scripts/`), so
the module under test is loaded via `importlib.util.spec_from_file_location`.
"""

from __future__ import annotations

import csv
import importlib.util
import random
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from csv_processor.config import load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_MODULE_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_CONFIGS_DIR = _REPO_ROOT / "configs"

_SPEC = importlib.util.spec_from_file_location("generate_csv", _MODULE_PATH)
generate_csv = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
# Register in sys.modules BEFORE exec_module: generate_csv.py's frozen
# dataclass (GeneratedCsv) uses postponed annotations, and dataclasses'
# forward-ref resolution looks the module up via sys.modules[cls.__module__]
# -- without this it raises AttributeError on a None module during class
# creation.
sys.modules["generate_csv"] = generate_csv
_SPEC.loader.exec_module(generate_csv)


@pytest.fixture
def customers_config():
    return load_config(
        _CONFIGS_DIR / "datasets" / "customers.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )


@pytest.fixture
def orders_config():
    return load_config(
        _CONFIGS_DIR / "datasets" / "orders.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )


def test_generate_rows_is_deterministic_for_same_seed(customers_config) -> None:
    first = generate_csv.generate_rows(customers_config, rows=50, invalid_ratio=0.2, seed=42)
    second = generate_csv.generate_rows(customers_config, rows=50, invalid_ratio=0.2, seed=42)

    assert first.header == second.header
    assert first.rows == second.rows
    assert first.categories == second.categories


def test_cli_run_twice_produces_byte_identical_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run1")
    generate_csv.main(["--dataset", "customers", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"])
    first_bytes = generate_csv.output_path("customers").read_bytes()

    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run2")
    generate_csv.main(["--dataset", "customers", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"])
    second_bytes = generate_csv.output_path("customers").read_bytes()

    assert first_bytes == second_bytes


def test_invalid_ratio_zero_produces_all_valid_rows(customers_config) -> None:
    generated = generate_csv.generate_rows(customers_config, rows=30, invalid_ratio=0.0, seed=1)

    assert len(generated.rows) == 30
    assert all(category is None for category in generated.categories)


def test_invalid_ratio_one_produces_all_invalid_rows(customers_config) -> None:
    generated = generate_csv.generate_rows(customers_config, rows=30, invalid_ratio=1.0, seed=1)

    assert len(generated.rows) == 30
    assert all(category is not None for category in generated.categories)


def test_invalid_ratio_boundaries_are_accepted_by_cli() -> None:
    parser = generate_csv.build_parser()

    args_zero = parser.parse_args(["--dataset", "customers", "--invalid-ratio", "0.0"])
    args_one = parser.parse_args(["--dataset", "customers", "--invalid-ratio", "1.0"])

    assert args_zero.invalid_ratio == 0.0
    assert args_one.invalid_ratio == 1.0


def test_invalid_ratio_outside_range_rejected() -> None:
    parser = generate_csv.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--dataset", "customers", "--invalid-ratio", "1.5"])


def test_zero_rows_produces_header_only_csv(customers_config, tmp_path) -> None:
    generated = generate_csv.generate_rows(customers_config, rows=0, invalid_ratio=0.1, seed=1)
    out_path = tmp_path / "customers.csv"

    generate_csv.write_csv(generated, customers_config, out_path)

    lines = out_path.read_text(encoding="utf-8").splitlines()
    assert generated.rows == []
    assert len(lines) == 1


def test_one_row_produces_exactly_one_data_row(customers_config, tmp_path) -> None:
    generated = generate_csv.generate_rows(customers_config, rows=1, invalid_ratio=0.1, seed=1)
    out_path = tmp_path / "customers.csv"

    generate_csv.write_csv(generated, customers_config, out_path)

    lines = out_path.read_text(encoding="utf-8").splitlines()
    assert len(generated.rows) == 1
    assert len(lines) == 2


def test_csv_header_matches_column_names_in_declared_order(customers_config) -> None:
    generated = generate_csv.generate_rows(customers_config, rows=5, invalid_ratio=0.0, seed=1)

    assert generated.header == [column.name for column in customers_config.columns]
    assert generated.header == [
        "customer_id",
        "name",
        "country",
        "birth_date",
        "event_ts",
        "signup_country",
    ]


def test_invalid_row_categories_are_restricted_to_applicable_categories(customers_config) -> None:
    generated = generate_csv.generate_rows(customers_config, rows=200, invalid_ratio=1.0, seed=7)

    applicable = set(generate_csv.applicable_categories(customers_config))
    used = {category for category in generated.categories if category is not None}

    assert used
    assert used <= applicable
    # customers.json has no integer/decimal/boolean column -- "wrong_type" must
    # never be applicable to, or used for, this dataset.
    assert "wrong_type" not in applicable
    assert "wrong_type" not in used


def test_format_decimal_uses_exact_scale_never_str_float() -> None:
    rng = random.Random(1)

    formatted = generate_csv.format_decimal(rng, precision=12, scale=2)

    assert "." in formatted
    decimal_places = len(formatted.split(".")[1])
    assert decimal_places == 2
    # Confirm it round-trips through Decimal cleanly (never str(float) drift).
    assert Decimal(formatted).as_tuple().exponent == -2


def test_cli_end_to_end_writes_real_csv_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path)

    exit_code = generate_csv.main(
        ["--dataset", "customers", "--rows", "20", "--invalid-ratio", "0.25", "--seed", "7"]
    )

    assert exit_code == 0
    out_path = generate_csv.output_path("customers")
    assert out_path.exists()
    with out_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        data_rows = list(reader)
    assert header == ["customer_id", "name", "country", "birth_date", "event_ts", "signup_country"]
    assert len(data_rows) == 20


# --- orders-specific tests (02-02 Task 1): proves the same generator/loader path
# works, unmodified, for a second dataset with a decimal AND a date column. ---


def test_generate_rows_is_deterministic_for_same_seed_orders(orders_config) -> None:
    first = generate_csv.generate_rows(orders_config, rows=50, invalid_ratio=0.2, seed=42)
    second = generate_csv.generate_rows(orders_config, rows=50, invalid_ratio=0.2, seed=42)

    assert first.header == second.header
    assert first.rows == second.rows
    assert first.categories == second.categories


def test_cli_run_twice_produces_byte_identical_files_orders(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run1")
    generate_csv.main(["--dataset", "orders", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"])
    first_bytes = generate_csv.output_path("orders").read_bytes()

    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run2")
    generate_csv.main(["--dataset", "orders", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"])
    second_bytes = generate_csv.output_path("orders").read_bytes()

    assert first_bytes == second_bytes


def test_orders_csv_header_matches_column_names_in_declared_order(orders_config) -> None:
    generated = generate_csv.generate_rows(orders_config, rows=5, invalid_ratio=0.0, seed=1)

    assert generated.header == ["order_id", "customer_id", "order_date", "amount"]


def test_orders_exercises_wrong_type_and_invalid_date_categories(orders_config) -> None:
    """`orders` has both a decimal column (`amount`) and a date column
    (`order_date`), so unlike `customers` it must be able to exercise both the
    `wrong_type` and `invalid_date` D-15 categories."""
    generated = generate_csv.generate_rows(orders_config, rows=200, invalid_ratio=0.5, seed=42)

    applicable = set(generate_csv.applicable_categories(orders_config))
    used = {category for category in generated.categories if category is not None}

    assert "wrong_type" in applicable
    assert "invalid_date" in applicable
    assert "wrong_type" in used
    assert "invalid_date" in used
    assert used <= applicable


def test_orders_valid_amount_values_have_exactly_two_decimal_places(orders_config) -> None:
    generated = generate_csv.generate_rows(orders_config, rows=200, invalid_ratio=0.2, seed=42)
    amount_index = generated.header.index("amount")

    valid_amounts = [
        row[amount_index]
        for row, category in zip(generated.rows, generated.categories)
        if category is None
    ]

    assert valid_amounts
    for amount in valid_amounts:
        assert Decimal(amount).as_tuple().exponent == -2


def test_cli_end_to_end_writes_real_csv_file_orders(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path)

    exit_code = generate_csv.main(
        ["--dataset", "orders", "--rows", "20", "--invalid-ratio", "0.25", "--seed", "7"]
    )

    assert exit_code == 0
    out_path = generate_csv.output_path("orders")
    assert out_path.exists()
    with out_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        data_rows = list(reader)
    assert header == ["order_id", "customer_id", "order_date", "amount"]
    assert len(data_rows) == 20
