"""Deterministic business-row CSV generator (GEN-01).

`--dataset`/`--rows`/`--invalid-ratio`/`--seed` (D-14) drive a `csv.writer`-based
CSV write. Determinism comes from `Faker.seed(seed)` for realistic-looking
string fields and a *separate* `random.Random(seed)` instance for which rows
are invalid, which of D-15's four invalid-row categories a given invalid row
uses, and every numeric/date value range -- so the two randomness streams
never interleave in a way that would make one depend on how many values the
other consumed.

Reads a dataset's validated config purely via
`csv_processor.config.loader.load_config` to learn column names/types/
nullability -- this is the CLI's ONLY dependency on `csv_processor`. It never
imports `csv_processor.detect`/`.validate`/`.normalize` (D-14's explicit
zero-coupling constraint; see 02-RESEARCH.md's "Pattern 3").
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from csv_processor.config import ColumnSpec, DatasetConfig, load_config
from faker import Faker

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_DATA_DIR = _REPO_ROOT / "data"

_NUMERIC_TYPES = ("integer", "decimal", "boolean")
_DATE_TYPES = ("date", "timestamp")

# D-15's four invalid-row categories, restricted per-dataset to what its
# schema can actually produce (see applicable_categories()).
_ALL_CATEGORIES = ("wrong_type", "invalid_date", "missing_required", "wrong_column_count")


def applicable_categories(config: DatasetConfig) -> list[str]:
    """The subset of D-15's four invalid-row categories this dataset's schema
    can actually exercise -- e.g. a schema with no integer/decimal/boolean
    column can never realistically produce a "wrong_type" row.
    """
    categories: list[str] = []
    if any(column.type in _NUMERIC_TYPES for column in config.columns):
        categories.append("wrong_type")
    if any(column.type in _DATE_TYPES for column in config.columns):
        categories.append("invalid_date")
    if any(not column.nullable for column in config.columns):
        categories.append("missing_required")
    categories.append("wrong_column_count")  # always applicable -- structural, not column-typed
    return categories


def format_decimal(rng: random.Random, precision: int, scale: int) -> str:
    """Format a random decimal value with exactly `scale` decimal places via
    explicit `Decimal` formatting -- never `str(float(...))` (D-16's R10
    discipline, applied here too even though R1-R10 formally govern the
    corpus subsystem).
    """
    max_value = 10**precision - 1
    value = Decimal(rng.randint(0, max_value)).scaleb(-scale)
    return f"{value:.{scale}f}"


def seed_component(seed: int, length: int = 8) -> str:
    """Deterministic hex fragment derived from `seed` -- the shared prefix
    baked into every structured ID this run produces (D-06/D-07)."""
    return hashlib.sha256(str(seed).encode("utf-8")).hexdigest()[:length]


def structured_id(prefix: str, seed: int, sequence: int, width: int = 6) -> str:
    """A deterministic, seed-derived structured ID:
    `{prefix}-{seed_component(seed)}-{sequence:0{width}d}` (D-06/D-08) --
    never a random Faker word, never approximate formatting (mirrors
    `format_decimal()`'s exact-`Decimal` discipline)."""
    return f"{prefix}-{seed_component(seed)}-{sequence:0{width}d}"


def zipf_weighted_sample(rng: random.Random, pool: list[str], k: int) -> list[str]:
    """Sample `k` values from `pool`, with replacement, weighted so earlier
    entries in `pool` are proportionally more likely to be drawn (weight ∝
    1/rank) -- D-02's with-replacement sampling, D-03's Zipf-like skew."""
    weights = [1.0 / (rank + 1) for rank in range(len(pool))]
    return rng.choices(pool, weights=weights, k=k)


def _fake_string_value(fake: Faker, column: ColumnSpec) -> str:
    name = column.name.lower()
    if name == "name":
        return fake.name()
    if "country" in name:
        return fake.country()
    return fake.word()


def _valid_value(fake: Faker, rng: random.Random, column: ColumnSpec) -> str:
    if column.type == "string":
        return _fake_string_value(fake, column)
    if column.type == "integer":
        return str(rng.randint(0, 1_000_000))
    if column.type == "decimal":
        if (
            column.precision is None or column.scale is None
        ):  # pragma: no cover - guarded by config validation
            msg = f"column {column.name!r}: decimal type missing precision/scale"
            raise ValueError(msg)
        return format_decimal(rng, column.precision, column.scale)
    if column.type == "boolean":
        return "true" if rng.random() < 0.5 else "false"
    if column.type == "date":
        if not column.format:  # pragma: no cover - guarded by config validation
            msg = f"column {column.name!r}: date type missing format"
            raise ValueError(msg)
        base = date(2000, 1, 1)
        value_date = base + timedelta(days=rng.randint(0, 365 * 25))
        return value_date.strftime(column.format)
    if column.type == "timestamp":
        if not column.format:  # pragma: no cover - guarded by config validation
            msg = f"column {column.name!r}: timestamp type missing format"
            raise ValueError(msg)
        base = datetime(2020, 1, 1, tzinfo=UTC)
        value_dt = base + timedelta(seconds=rng.randint(0, 365 * 5 * 86400))
        return value_dt.strftime(column.format)
    msg = f"column {column.name!r}: unsupported column type {column.type!r}"
    raise ValueError(msg)


def _generate_valid_row(
    fake: Faker,
    rng: random.Random,
    columns: list[ColumnSpec],
    overrides: dict[str, str] | None = None,
) -> list[str]:
    """`overrides` (D-01/D-06/D-08's correlated/structured IDs) wins over
    `_valid_value()` for a matched column name -- assigned BEFORE any
    invalid-category corruption (`_generate_invalid_row`) runs on top."""
    return [
        overrides[column.name]
        if overrides is not None and column.name in overrides
        else _valid_value(fake, rng, column)
        for column in columns
    ]


def _generate_invalid_row(
    fake: Faker,
    rng: random.Random,
    columns: list[ColumnSpec],
    category: str,
    overrides: dict[str, str] | None = None,
) -> list[str]:
    row = _generate_valid_row(fake, rng, columns, overrides)
    if category == "wrong_type":
        numeric_indices = [i for i, column in enumerate(columns) if column.type in _NUMERIC_TYPES]
        row[rng.choice(numeric_indices)] = "not-a-number"
    elif category == "invalid_date":
        date_indices = [i for i, column in enumerate(columns) if column.type in _DATE_TYPES]
        row[rng.choice(date_indices)] = "not-a-date"
    elif category == "missing_required":
        required_indices = [i for i, column in enumerate(columns) if not column.nullable]
        row[rng.choice(required_indices)] = ""
    elif category == "wrong_column_count":
        row = row[:-1]  # one fewer field than the header
    else:  # pragma: no cover - defensive, category always comes from applicable_categories()
        msg = f"unknown invalid category: {category!r}"
        raise ValueError(msg)
    return row


@dataclass(frozen=True)
class GeneratedCsv:
    """The in-memory result of `generate_rows()` -- header, row values, and a
    parallel `categories` list (`None` for a valid row, else the invalid
    category used) so callers/tests can inspect what was generated without
    re-parsing written CSV bytes.
    """

    header: list[str]
    rows: list[list[str]]
    categories: list[str | None]


def generate_rows(
    config: DatasetConfig,
    rows: int,
    invalid_ratio: float,
    seed: int,
    *,
    rng: random.Random | None = None,
    fake: Faker | None = None,
    customer_id_pool: list[str] | None = None,
) -> GeneratedCsv:
    """Generate `rows` deterministic rows for `config`, `invalid_ratio` of them
    invalid across D-15's applicable categories.

    Faker.seed(seed) drives realistic-looking string values; a *separate*
    random.Random(seed) drives which rows are invalid, which category each
    invalid row uses, and every numeric/date value range.

    `rng`/`fake` are optional, keyword-only overrides (PD-1) -- when omitted
    (every pre-existing caller), constructed internally exactly as before.
    `generate_correlated_datasets()` passes the SAME live `rng`/`fake` pair
    into both its customers and orders calls, giving literal object-identity
    RNG continuation across that boundary (D-05).

    `customer_id_pool` (PD-2) only affects a config that declares a
    `customer_id` column: `None` (the default -- used for `customers_config`,
    which owns customer identity) assigns sequential structured IDs; a
    non-empty list (used only for `orders_config` by
    `generate_correlated_datasets()`) assigns Zipf-weighted, with-replacement
    pool samples instead (D-01-D-03).
    """
    if fake is None:
        fake = Faker()
        Faker.seed(seed)
    if rng is None:
        rng = random.Random(seed)

    categories = applicable_categories(config)
    header = [column.name for column in config.columns]
    column_names = [column.name for column in config.columns]

    id_overrides: dict[str, list[str]] = {}
    if "customer_id" in column_names:
        if customer_id_pool is not None:
            if not customer_id_pool:
                msg = "cannot generate rows: customer_id_pool is empty (D-04)"
                raise ValueError(msg)
            id_overrides["customer_id"] = zipf_weighted_sample(rng, customer_id_pool, rows)
        else:
            id_overrides["customer_id"] = [
                structured_id("CUST", seed, i + 1, width=6) for i in range(rows)
            ]
    if "order_id" in column_names:
        # D-08: order_id is always structured, never pool-sampled -- it is
        # never a foreign reference.
        id_overrides["order_id"] = [structured_id("ORD", seed, i + 1, width=6) for i in range(rows)]

    num_invalid = min(round(rows * invalid_ratio), rows)
    invalid_indices = set(rng.sample(range(rows), num_invalid)) if rows > 0 else set()

    out_rows: list[list[str]] = []
    row_categories: list[str | None] = []
    for i in range(rows):
        overrides = {name: values[i] for name, values in id_overrides.items()} or None
        if i in invalid_indices:
            category = rng.choice(categories)
            out_rows.append(_generate_invalid_row(fake, rng, config.columns, category, overrides))
            row_categories.append(category)
        else:
            out_rows.append(_generate_valid_row(fake, rng, config.columns, overrides))
            row_categories.append(None)

    return GeneratedCsv(header=header, rows=out_rows, categories=row_categories)


def write_csv(generated: GeneratedCsv, config: DatasetConfig, path: Path) -> None:
    """Write `generated` to `path` using `config.csv`'s own dialect fields --
    never a hardcoded quoting/delimiter convention (D-01/D-02/D-03)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding=config.csv.encoding) as handle:
        writer = csv.writer(
            handle,
            delimiter=config.csv.delimiter,
            quotechar=config.csv.quotechar,
            escapechar=config.csv.escapechar,
            doublequote=config.csv.doublequote,
            lineterminator=config.csv.lineterminator,
            quoting=csv.QUOTE_MINIMAL,
        )
        if config.csv.header:
            writer.writerow(generated.header)
        writer.writerows(generated.rows)


@dataclass(frozen=True)
class CorrelatedDatasets:
    """The paired result of `generate_correlated_datasets()` -- `orders`'
    `customer_id` values are a real, Zipf-weighted, with-replacement sample
    from `customers`' own valid-row pool, never independently random
    (D-01-D-05)."""

    customers: GeneratedCsv
    orders: GeneratedCsv


def generate_correlated_datasets(
    customers_config: DatasetConfig,
    orders_config: DatasetConfig,
    *,
    customers_rows: int,
    orders_rows: int,
    invalid_ratio: float,
    seed: int,
) -> CorrelatedDatasets:
    """Generate a customers/orders pair where `orders.customer_id` is drawn
    from the pool of `customer_id` values that will land in
    `customers_valid` -- the core fix this phase exists for (D-01-D-08).

    Constructs ONE `fake`/`rng` pair and passes the SAME live objects into
    both the customers and orders `generate_rows()` calls (PD-1's literal
    object-identity RNG continuation, satisfying D-05's "same seeded
    `random.Random(seed)` instance" by its most literal reading).

    Raises `ValueError` if generating `customers_config` at
    `customers_rows`/`invalid_ratio` would leave zero valid customer rows
    (D-04) -- checked here, before ever calling `generate_rows()` for
    orders, rather than relying solely on `generate_rows()`'s own
    defense-in-depth empty-pool check.
    """
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    customers_generated = generate_rows(
        customers_config, customers_rows, invalid_ratio, seed, rng=rng, fake=fake
    )

    customer_id_index = customers_generated.header.index("customer_id")
    valid_customer_pool = [
        row[customer_id_index]
        for row, category in zip(
            customers_generated.rows, customers_generated.categories, strict=True
        )
        if category is None
    ]
    if not valid_customer_pool:
        msg = "cannot generate correlated orders: valid-customer pool is empty"
        raise ValueError(msg)

    orders_generated = generate_rows(
        orders_config,
        orders_rows,
        invalid_ratio,
        seed,
        rng=rng,
        fake=fake,
        customer_id_pool=valid_customer_pool,
    )

    return CorrelatedDatasets(customers=customers_generated, orders=orders_generated)


def _ratio_type(value: str) -> float:
    parsed = float(value)
    if not (0.0 <= parsed <= 1.0):
        msg = f"--invalid-ratio must be within [0.0, 1.0], got {parsed}"
        raise argparse.ArgumentTypeError(msg)
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a deterministic business-row CSV fixture for one dataset (GEN-01)."
    )
    parser.add_argument("--dataset", required=True, help="Dataset name, e.g. 'customers'.")
    parser.add_argument("--rows", type=int, default=100, help="Number of data rows to generate.")
    parser.add_argument(
        "--invalid-ratio",
        type=_ratio_type,
        default=0.1,
        help="Fraction of rows (0.0-1.0 inclusive) that are deliberately invalid.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260101,
        help="Seed for both Faker and the generator's own random.Random instance.",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Gzip the generated CSV after writing (D-32).",
    )
    return parser


def output_path(dataset: str, *, today: date | None = None) -> Path:
    """`./data/<dataset>/<dataset>_<YYYYMMDD>.csv` (D-06/D-07)."""
    day = today or date.today()
    return _DATA_DIR / dataset / f"{dataset}_{day:%Y%m%d}.csv"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(
        _CONFIGS_DIR / "datasets" / f"{args.dataset}.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    generated = generate_rows(config, args.rows, args.invalid_ratio, args.seed)
    path = output_path(args.dataset)
    write_csv(generated, config, path)
    if args.compress:
        # D-32: gzip the just-written CSV, then remove the plain file --
        # mirrors the `gzip` CLI tool's own in-place-replace behavior, and
        # matches D-31's widened file_pattern ("customers_*.csv*") expecting
        # exactly one file per drop, not both a plain and compressed variant
        # sitting side by side.
        gz_path = path.with_name(f"{path.name}.gz")
        with gzip.open(gz_path, "wb") as gz_handle:
            gz_handle.write(path.read_bytes())
        path.unlink()
        print(f"compressed to {gz_path}")
        return 0
    print(f"wrote {len(generated.rows)} rows to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
