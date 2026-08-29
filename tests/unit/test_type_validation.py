"""Proves corpus fixtures 17-22 (``type_nullability`` category) against
``csv_processor.engine.process_chunks()`` (03-03-PLAN.md Task 2).

Fixture 18's header (``order_id, customer_id, order_date, amount``) is
identical to ``orders.json``'s real declared column set, so it runs against
the real, loaded ``orders.json`` config directly.

Fixtures 17/19/20/21/22 each declare a header that is a genuine SUBSET (or,
for 17, a partial replacement) of ``orders.json``/``customers.json``'s real
column set -- e.g. fixture 17's ``quantity`` column has no equivalent in
either real dataset config at all, and fixture 19's three-column header
omits three of ``customers.json``'s six declared columns. Routing these
through the REAL ``customers.json``/``orders.json`` config would trip a
whole-file ``MISSING_REQUIRED_COLUMN``/``EXTRA_UNEXPECTED_COLUMN``
structural reject before a single row's type/nullability check ever ran --
exactly the schema-mismatch trap 03-RESEARCH.md's Pitfall 3 documents for
the ``byte_level_hard`` category, which applies here too. Each such fixture
instead gets its own small, fixture-scoped ``DatasetConfig`` -- built with
the exact per-column type/nullable/format/precision/scale semantics carried
over from the real ``customers``/``orders`` schema for every column name
they share (D-12's error-code vocabulary is schema-shape-driven, not
schema-identity-driven) -- so the fixture's own declared header is what the
structural check succeeds against, and its data row reaches the real
type/nullability check this task proves.
"""

from __future__ import annotations

from pathlib import Path

from csv_processor.config.loader import load_config
from csv_processor.config.models import (
    ColumnSpec,
    CsvDialectConfig,
    DatasetConfig,
    OracleTargetSpec,
    ProcessingConfig,
)
from csv_processor.engine import process_chunks

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")
_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_ORDERS_PATH = _CONFIGS_DIR / "datasets" / "orders.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def _ad_hoc_config(*, dataset: str, columns: list[ColumnSpec]) -> DatasetConfig:
    """Build a small fixture-scoped config -- see module docstring."""
    return DatasetConfig(
        dataset=dataset,
        file_pattern=f"{dataset}_*.csv",
        csv=CsvDialectConfig(),
        columns=columns,
        oracle=OracleTargetSpec(valid_table=f"{dataset}_valid", invalid_table=f"{dataset}_invalid"),
        processing=ProcessingConfig(chunk_size=5000),
    )


def _write_fixture(tmp_path: Path, name: str, filename: str) -> Path:
    csv_path = tmp_path / filename
    csv_path.write_bytes(_fixture_bytes(name))
    return csv_path


def test_17_invalid_integer_value(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "17_invalid_integer_value", "orders_17.csv")
    config = _ad_hoc_config(
        dataset="orders",
        columns=[
            ColumnSpec(name="order_id", type="string", nullable=False, required=True),
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(name="quantity", type="integer", nullable=False, required=True),
            ColumnSpec(
                name="amount", type="decimal", nullable=True, required=True, precision=12, scale=2
            ),
        ],
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert valid_rows == []
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "TYPE_MISMATCH"
    assert invalid_rows[0]["quantity"] == "not-a-number"


def test_18_invalid_decimal_too_many_places(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "18_invalid_decimal_too_many_places", "orders_18.csv")
    config = load_config(_ORDERS_PATH, defaults_path=_DEFAULTS_PATH)

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert valid_rows == []
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "DECIMAL_PRECISION_EXCEEDED"
    assert invalid_rows[0]["amount"] == "100.999"


def test_19_invalid_date_format(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "19_invalid_date_format", "customers_19.csv")
    config = _ad_hoc_config(
        dataset="customers",
        columns=[
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(name="name", type="string", nullable=False, required=True),
            ColumnSpec(
                name="birth_date",
                type="date",
                nullable=True,
                required=True,
                format="%Y-%m-%d",
            ),
        ],
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert valid_rows == []
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "INVALID_DATE_FORMAT"
    assert invalid_rows[0]["birth_date"] == "31/02/2026"


def test_20_invalid_timestamp_format(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "20_invalid_timestamp_format", "customers_20.csv")
    config = _ad_hoc_config(
        dataset="customers",
        columns=[
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(
                name="event_ts",
                type="timestamp",
                nullable=False,
                required=True,
                format="%Y-%m-%dT%H:%M:%S%z",
            ),
        ],
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert valid_rows == []
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "INVALID_TIMESTAMP_FORMAT"
    assert invalid_rows[0]["event_ts"] == "2026-01-01 00:00:00"


def test_21_empty_required_field(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "21_empty_required_field", "customers_21.csv")
    config = _ad_hoc_config(
        dataset="customers",
        columns=[
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(name="name", type="string", nullable=False, required=True),
            ColumnSpec(name="country", type="string", nullable=False, required=True),
        ],
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert valid_rows == []
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "NULL_VIOLATION"
    assert invalid_rows[0]["customer_id"] == ""


def test_22_empty_nullable_field_should_pass(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "22_empty_nullable_field_should_pass", "customers_22.csv")
    config = _ad_hoc_config(
        dataset="customers",
        columns=[
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(name="name", type="string", nullable=False, required=True),
            ColumnSpec(name="signup_country", type="string", nullable=True, required=False),
        ],
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert valid_rows[0]["signup_country"] is None
