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
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from faker import Faker

from csv_processor.config import ColumnSpec, DatasetConfig, load_config

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
        if column.precision is None or column.scale is None:  # pragma: no cover - guarded by config validation
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
        base = datetime(2020, 1, 1, tzinfo=timezone.utc)
        value_dt = base + timedelta(seconds=rng.randint(0, 365 * 5 * 86400))
        return value_dt.strftime(column.format)
    msg = f"column {column.name!r}: unsupported column type {column.type!r}"
    raise ValueError(msg)


def _generate_valid_row(fake: Faker, rng: random.Random, columns: list[ColumnSpec]) -> list[str]:
    return [_valid_value(fake, rng, column) for column in columns]


def _generate_invalid_row(
    fake: Faker,
    rng: random.Random,
    columns: list[ColumnSpec],
    category: str,
) -> list[str]:
    row = _generate_valid_row(fake, rng, columns)
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


def generate_rows(config: DatasetConfig, rows: int, invalid_ratio: float, seed: int) -> GeneratedCsv:
    """Generate `rows` deterministic rows for `config`, `invalid_ratio` of them
    invalid across D-15's applicable categories.

    Faker.seed(seed) drives realistic-looking string values; a *separate*
    random.Random(seed) drives which rows are invalid, which category each
    invalid row uses, and every numeric/date value range.
    """
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    categories = applicable_categories(config)
    header = [column.name for column in config.columns]

    num_invalid = min(round(rows * invalid_ratio), rows)
    invalid_indices = set(rng.sample(range(rows), num_invalid)) if rows > 0 else set()

    out_rows: list[list[str]] = []
    row_categories: list[str | None] = []
    for i in range(rows):
        if i in invalid_indices:
            category = rng.choice(categories)
            out_rows.append(_generate_invalid_row(fake, rng, config.columns, category))
            row_categories.append(category)
        else:
            out_rows.append(_generate_valid_row(fake, rng, config.columns))
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
