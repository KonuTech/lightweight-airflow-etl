"""End-to-end tracer: one valid, one invalid ``customers`` row through the
complete detect -> parse -> validate -> normalize -> split pipeline
(03-03-PLAN.md Task 1 -- the plan's own leading tracer slice).

Proves ``csv_processor.engine.process_chunks()`` wires ``source.py``,
``validate.py``, and ``normalize.py`` together correctly for the first
time in this phase, against a real, loaded ``customers.json`` config --
not just "no exception raised".
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from csv_processor.config.loader import load_config
from csv_processor.engine import process_chunks

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"

_TRACER_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
    ",Bob Jones,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
)


def test_one_valid_one_invalid_customers_row_end_to_end(tmp_path: Path) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_TRACER_CSV, encoding="utf-8")
    config = load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 1
    assert len(invalid_rows) == 1

    valid_row = valid_rows[0]
    assert valid_row == {
        "customer_id": "CUST001",
        "name": "Alice Smith",
        "country": "DE",
        "birth_date": dt.date(1990, 1, 1),
        "event_ts": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        "signup_country": "FR",
    }
    assert isinstance(valid_row["birth_date"], dt.date)
    assert not isinstance(valid_row["birth_date"], dt.datetime)
    assert isinstance(valid_row["event_ts"], dt.datetime)

    invalid_row = invalid_rows[0]
    assert invalid_row["error_code"] == "NULL_VIOLATION"
    assert invalid_row["customer_id"] == ""
    assert invalid_row["row_number"] == 2
    assert invalid_row["source_file"] == "customers_20260829.csv"
    assert "raw_line" in invalid_row
    assert invalid_row["raw_line"].startswith(",Bob Jones,DE,")


def test_structurally_broken_row_never_reaches_check_row(tmp_path: Path) -> None:
    """A wrong-field-count row's error_code is always WRONG_COLUMN_COUNT,
    never a type/nullability code -- proves the D-13 short-circuit.

    Two well-formed 6-field rows surround the ragged 3-field row so
    ``detect_header``'s modal-field-count heuristic still recognizes row 0
    as the header (it needs real following-row context, per
    03-RESEARCH.md's own header-detection note) -- a lone ragged row with
    nothing else to compare against would otherwise be misdetected as the
    header itself.
    """
    csv_path = tmp_path / "customers_ragged.csv"
    csv_path.write_text(
        "customer_id,name,country,birth_date,event_ts,signup_country\n"
        "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
        "CUST002,Bob Jones,DE\n"
        "CUST003,Carol White,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n",
        encoding="utf-8",
    )
    config = load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 2
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert invalid_rows[0]["row_number"] == 2


def test_convert_value_decimal_precision_exceeded() -> None:
    from csv_processor.config.models import ColumnSpec
    from csv_processor.normalize import convert_value

    column = ColumnSpec(
        name="amount", type="decimal", nullable=True, required=True, precision=12, scale=2
    )
    value, error_code = convert_value("100.999", column)

    assert value is None
    assert error_code == "DECIMAL_PRECISION_EXCEEDED"


def test_convert_value_invalid_date_format() -> None:
    from csv_processor.config.models import ColumnSpec
    from csv_processor.normalize import convert_value

    column = ColumnSpec(
        name="birth_date", type="date", nullable=True, required=True, format="%Y-%m-%d"
    )
    value, error_code = convert_value("31/02/2026", column)

    assert value is None
    assert error_code == "INVALID_DATE_FORMAT"
