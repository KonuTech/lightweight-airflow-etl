"""Tests for generator/generate_csv.py (GEN-01), scoped to `--dataset customers`
(Plan 02 extends coverage to `orders`).

`generator/` has no `__init__.py` and is not an installed package (matches this
project's own `tests/test_verify_environment.py` convention for `scripts/`), so
the module under test is loaded via `importlib.util.spec_from_file_location`.
"""

from __future__ import annotations

import csv
import gzip
import importlib.util
import random
import re
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest
from csv_processor.config import load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_MODULE_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_CONFIGS_DIR = _REPO_ROOT / "configs"

_SPEC = importlib.util.spec_from_file_location("generate_csv", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
generate_csv = importlib.util.module_from_spec(_SPEC)
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
    generate_csv.main(
        ["--dataset", "customers", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"]
    )
    first_bytes = generate_csv.output_path("customers").read_bytes()

    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run2")
    generate_csv.main(
        ["--dataset", "customers", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"]
    )
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
    """D-05: a --correlated run is byte-identical for both customers and
    orders across two runs with the same seed (replaces the old
    standalone-orders-only assertion, which --correlated now supersedes)."""
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run1")
    generate_csv.main(["--correlated", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"])
    first_customers_bytes = generate_csv.output_path("customers").read_bytes()
    first_orders_bytes = generate_csv.output_path("orders").read_bytes()

    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path / "run2")
    generate_csv.main(["--correlated", "--rows", "50", "--invalid-ratio", "0.2", "--seed", "42"])
    second_customers_bytes = generate_csv.output_path("customers").read_bytes()
    second_orders_bytes = generate_csv.output_path("orders").read_bytes()

    assert first_customers_bytes == second_customers_bytes
    assert first_orders_bytes == second_orders_bytes


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
        for row, category in zip(generated.rows, generated.categories, strict=True)
        if category is None
    ]

    assert valid_amounts
    for amount in valid_amounts:
        assert Decimal(amount).as_tuple().exponent == -2


def test_cli_end_to_end_writes_real_csv_file_orders(tmp_path, monkeypatch) -> None:
    """D-22: orders is now reached via --correlated, not a standalone
    --dataset orders call (which test_bare_dataset_orders_cli_is_rejected
    below now proves is rejected)."""
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path)

    exit_code = generate_csv.main(
        ["--correlated", "--rows", "20", "--invalid-ratio", "0.25", "--seed", "7"]
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


def test_bare_dataset_orders_cli_is_rejected() -> None:
    """D-22's CLI-level enforcement: orders can no longer be generated
    independently -- --dataset orders alone (no --correlated) must exit
    non-zero via argparse's own SystemExit."""
    with pytest.raises(SystemExit):
        generate_csv.main(["--dataset", "orders"])


# --- --compress flag (D-32, 03-04-PLAN.md Task 3) --------------------------


def test_compress_flag_produces_gz_file_and_removes_plain_csv(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path)

    exit_code = generate_csv.main(
        [
            "--dataset",
            "customers",
            "--rows",
            "20",
            "--invalid-ratio",
            "0.1",
            "--seed",
            "7",
            "--compress",
        ]
    )

    assert exit_code == 0
    plain_path = generate_csv.output_path("customers")
    gz_path = plain_path.with_name(f"{plain_path.name}.gz")
    assert gz_path.exists()
    assert not plain_path.exists()


def test_compress_flag_produces_valid_gzipped_csv_matching_schema(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(generate_csv, "_DATA_DIR", tmp_path)

    generate_csv.main(
        [
            "--dataset",
            "customers",
            "--rows",
            "20",
            "--invalid-ratio",
            "0.0",
            "--seed",
            "7",
            "--compress",
        ]
    )

    plain_path = generate_csv.output_path("customers")
    gz_path = plain_path.with_name(f"{plain_path.name}.gz")

    with gzip.open(gz_path, "rt", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        data_rows = list(reader)

    assert header == ["customer_id", "name", "country", "birth_date", "event_ts", "signup_country"]
    assert len(data_rows) == 20


def test_compress_flag_defaults_to_false() -> None:
    parser = generate_csv.build_parser()

    args = parser.parse_args(["--dataset", "customers"])

    assert args.compress is False


# --- generate_correlated_datasets() (07-01-PLAN.md Task 2): correlation
# properties covering D-01/D-03/D-04/D-05/D-06/D-08 ---------------------


def test_correlated_orders_customer_id_is_subset_of_valid_customer_pool(
    customers_config, orders_config
) -> None:
    """D-01: orders.customer_id values are drawn from the pool of customer_id
    values that will land in customers_valid. invalid_ratio=0.0 avoids the
    documented (PD-2) missing_required corruption case, which can validly
    blank a pooled/structured ID on an INVALID row -- unrelated to this
    pool-sourcing property."""
    correlated = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=20,
        orders_rows=50,
        invalid_ratio=0.0,
        seed=42,
    )

    customer_id_index = correlated.customers.header.index("customer_id")
    valid_customer_pool = {
        row[customer_id_index]
        for row, category in zip(
            correlated.customers.rows, correlated.customers.categories, strict=True
        )
        if category is None
    }

    order_customer_id_index = correlated.orders.header.index("customer_id")
    order_customer_ids = {row[order_customer_id_index] for row in correlated.orders.rows}

    assert order_customer_ids
    assert order_customer_ids <= valid_customer_pool


def test_correlated_orders_customer_id_sampling_is_zipf_weighted(
    customers_config, orders_config
) -> None:
    """D-03: weight ∝ 1/rank means the most-sampled customer_id should appear
    strictly more often than the median count across all sampled
    customer_ids -- a small pool (20 customers) and a large sample
    (500 orders) makes the skew unambiguous."""
    correlated = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=20,
        orders_rows=500,
        invalid_ratio=0.0,
        seed=42,
    )

    order_customer_id_index = correlated.orders.header.index("customer_id")
    order_customer_ids = [row[order_customer_id_index] for row in correlated.orders.rows]

    counts = sorted(Counter(order_customer_ids).values())
    median_count = counts[len(counts) // 2]
    max_count = counts[-1]

    assert max_count > median_count


def test_generate_correlated_datasets_is_deterministic_for_same_seed(
    customers_config, orders_config
) -> None:
    """D-05: same seed produces byte-identical CorrelatedDatasets (header/
    rows/categories for BOTH customers and orders) across two separate
    calls."""
    first = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=20,
        orders_rows=50,
        invalid_ratio=0.2,
        seed=42,
    )
    second = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=20,
        orders_rows=50,
        invalid_ratio=0.2,
        seed=42,
    )

    assert first.customers.header == second.customers.header
    assert first.customers.rows == second.customers.rows
    assert first.customers.categories == second.customers.categories
    assert first.orders.header == second.orders.header
    assert first.orders.rows == second.orders.rows
    assert first.orders.categories == second.orders.categories


def test_generate_correlated_datasets_raises_on_empty_valid_customer_pool(
    customers_config, orders_config
) -> None:
    """D-04: every customer row invalid (invalid_ratio=1.0) leaves the
    valid-customer pool empty -- must raise immediately rather than silently
    falling back to independent random IDs."""
    with pytest.raises(ValueError, match="valid-customer pool is empty"):
        generate_csv.generate_correlated_datasets(
            customers_config,
            orders_config,
            customers_rows=5,
            orders_rows=1,
            invalid_ratio=1.0,
            seed=1,
        )


def test_correlated_ids_match_structured_id_format(customers_config, orders_config) -> None:
    """D-06/D-08: customer_id/order_id are seed-derived structured IDs, never
    a random Faker word. invalid_ratio=0.0 so every row's ID is genuinely
    assigned (not blanked by missing_required corruption), making the
    "every row" claim unambiguous."""
    correlated = generate_csv.generate_correlated_datasets(
        customers_config,
        orders_config,
        customers_rows=20,
        orders_rows=50,
        invalid_ratio=0.0,
        seed=42,
    )

    customer_id_index = correlated.customers.header.index("customer_id")
    for row in correlated.customers.rows:
        assert re.fullmatch(r"CUST-[0-9a-f]{8}-\d{6}", row[customer_id_index])

    order_customer_id_index = correlated.orders.header.index("customer_id")
    for row in correlated.orders.rows:
        assert re.fullmatch(r"CUST-[0-9a-f]{8}-\d{6}", row[order_customer_id_index])

    order_id_index = correlated.orders.header.index("order_id")
    for row in correlated.orders.rows:
        assert re.fullmatch(r"ORD-[0-9a-f]{8}-\d{6}", row[order_id_index])
