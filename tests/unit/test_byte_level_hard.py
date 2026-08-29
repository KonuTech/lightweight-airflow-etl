"""Proves corpus fixtures 23-27 (``byte_level_hard`` category) parse
correctly through ``csv_processor.engine.process_chunks()`` (03-05-PLAN.md
Task 2) -- the one corpus category no prior Phase 3 plan exercises.

Every fixture here is run against its OWN fixture-local ad hoc
``DatasetConfig`` -- never the real ``customers.json``/``orders.json``, per
03-RESEARCH.md Pitfall 3. Fixtures 23/24/25/27 declare a 2-column header
(``order_id, note`` or ``order_id, big_field``) that genuinely does NOT
match either real dataset's full declared column set (``orders.json``
requires ``order_id, customer_id, order_date, amount``); routing them
through the real config would trip a whole-file
``MISSING_REQUIRED_COLUMN``/``EXTRA_UNEXPECTED_COLUMN`` structural reject
before a single byte of their actual RFC-4180 content is ever reached --
exactly Pitfall 3's documented trap. Fixture 26's header (``order_id,
customer_id, order_date, amount``) happens to match ``orders.json``'s real
column set exactly and would NOT hit Pitfall 3's rejection -- it is still
exercised via its own fixture-local ad hoc config here for consistency with
the rest of this file, not because it needs one to pass structurally.
"""

from __future__ import annotations

from pathlib import Path

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


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def _order_id_note_config() -> DatasetConfig:
    """Fixture-local ad hoc config matching fixtures 23/24/25's actual
    declared header (``order_id, note``) -- never ``orders.json``'s real
    4-column schema (Pitfall 3)."""
    return DatasetConfig(
        dataset="byte_level_hard",
        file_pattern="byte_level_hard_*.csv",
        csv=CsvDialectConfig(),
        columns=[
            ColumnSpec(name="order_id", type="string", nullable=False, required=True),
            ColumnSpec(name="note", type="string", nullable=False, required=True),
        ],
        oracle=OracleTargetSpec(valid_table="bh_valid", invalid_table="bh_invalid"),
        processing=ProcessingConfig(chunk_size=10),
    )


def test_23_embedded_newline_in_quoted_field_survives_as_one_field(tmp_path: Path) -> None:
    csv_path = tmp_path / "orders_23.csv"
    csv_path.write_bytes(_fixture_bytes("23_embedded_newline_in_quoted_field"))
    config = _order_id_note_config()

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert valid_rows[0]["note"] == "line one\nline two"


def test_24_embedded_delimiter_in_quoted_field_survives_as_one_field(tmp_path: Path) -> None:
    csv_path = tmp_path / "orders_24.csv"
    csv_path.write_bytes(_fixture_bytes("24_embedded_delimiter_in_quoted_field"))
    config = _order_id_note_config()

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert valid_rows[0]["note"] == "contains, a comma"


def test_25_doubled_quote_escaping_unescapes_to_one_literal_quote(tmp_path: Path) -> None:
    csv_path = tmp_path / "orders_25.csv"
    csv_path.write_bytes(_fixture_bytes("25_doubled_quote_escaping"))
    config = _order_id_note_config()

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert valid_rows[0]["note"] == 'she said "hi" again'


def test_26_embedded_nul_byte_parses_without_raising(tmp_path: Path) -> None:
    """The ``customer_id`` field's raw NUL (0x00) byte survives as a literal
    ``"\\x00"`` character in the parsed field string -- no exception, per
    ``csv.reader``'s documented behavior over embedded NUL bytes."""
    csv_path = tmp_path / "orders_26.csv"
    csv_path.write_bytes(_fixture_bytes("26_embedded_nul_byte"))
    config = DatasetConfig(
        dataset="byte_level_hard",
        file_pattern="byte_level_hard_*.csv",
        csv=CsvDialectConfig(),
        columns=[
            ColumnSpec(name="order_id", type="string", nullable=False, required=True),
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(
                name="order_date", type="date", nullable=True, required=True, format="%Y-%m-%d"
            ),
            ColumnSpec(
                name="amount", type="decimal", nullable=True, required=True, precision=12, scale=2
            ),
        ],
        oracle=OracleTargetSpec(valid_table="bh_valid", invalid_table="bh_invalid"),
        processing=ProcessingConfig(chunk_size=10),
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert valid_rows[0]["customer_id"] == "CUST\x00001"
    assert "\x00" in valid_rows[0]["customer_id"]


def test_27_oversized_field_value_parses_as_a_single_unsplit_field(tmp_path: Path) -> None:
    """10,001 characters, well within source.py's 1 MiB
    ``csv.field_size_limit`` -- a parser-robustness smoke test only (no
    ``ColumnSpec.max_length`` field exists to drive a real rejection code,
    per 03-RESEARCH.md Open Question 2's resolution): asserts a successful
    parse, not an error_code."""
    csv_path = tmp_path / "orders_27.csv"
    csv_path.write_bytes(_fixture_bytes("27_oversized_field_value"))
    config = DatasetConfig(
        dataset="byte_level_hard",
        file_pattern="byte_level_hard_*.csv",
        csv=CsvDialectConfig(),
        columns=[
            ColumnSpec(name="order_id", type="string", nullable=False, required=True),
            ColumnSpec(name="big_field", type="string", nullable=False, required=True),
        ],
        oracle=OracleTargetSpec(valid_table="bh_valid", invalid_table="bh_invalid"),
        processing=ProcessingConfig(chunk_size=10),
    )

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert len(str(valid_rows[0]["big_field"])) == 10_001
    assert valid_rows[0]["big_field"] == "x" * 10_001
