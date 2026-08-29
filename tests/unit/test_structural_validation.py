"""Proves corpus fixtures 9-16 (``structural`` category) against
``csv_processor.source.prepare_source``/``csv_processor.engine.process_chunks``
(03-03-PLAN.md Task 3) -- every header-level whole-file-reject rule
(missing/extra/duplicate declared column, no header row, empty file) and
the row-level ``WRONG_COLUMN_COUNT`` rule.

Fixtures 9/10/13/14/16 declare a header matching (or a deliberate
single-column deviation from) ``customers.json``/``orders.json``'s real
column set exactly, so each runs against the real, loaded dataset config.
Fixture 11's duplicate-column-name check fires inside ``detect_header``
itself, before any comparison against a config's declared columns, so
which real config is passed is immaterial to the outcome -- ``orders.json``
is used since the fixture's header is orders-shaped. Fixture 15's headerless
content is also orders-shaped.

``process_chunks()`` is a generator -- calling it does not execute anything
until iteration begins, so every ``StructuralValidationError`` assertion
below wraps ``list(process_chunks(...))``, not the bare call.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from csv_processor.config.loader import load_config
from csv_processor.engine import process_chunks
from csv_processor.errors import StructuralValidationError

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")
_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_ORDERS_PATH = _CONFIGS_DIR / "datasets" / "orders.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def _write_fixture(tmp_path: Path, name: str, filename: str) -> Path:
    csv_path = tmp_path / filename
    csv_path.write_bytes(_fixture_bytes(name))
    return csv_path


def _customers_config():
    return load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)


def _orders_config():
    return load_config(_ORDERS_PATH, defaults_path=_DEFAULTS_PATH)


def test_09_missing_column(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "09_missing_column", "customers_09.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _customers_config()))

    assert exc.value.context["error_code"] == "MISSING_REQUIRED_COLUMN"


def test_10_extra_unexpected_column(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "10_extra_unexpected_column", "customers_10.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _customers_config()))

    assert exc.value.context["error_code"] == "EXTRA_UNEXPECTED_COLUMN"


def test_11_duplicate_column_name(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "11_duplicate_column_name", "orders_11.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _orders_config()))

    assert exc.value.context["error_code"] == "DUPLICATE_COLUMN_NAME"


def test_12_wrong_column_count_row(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "12_wrong_column_count_row", "orders_12.csv")

    chunks = list(process_chunks(csv_path, _orders_config()))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 2
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert invalid_rows[0]["row_number"] == 2
    assert valid_rows[0]["order_id"] == "ORD0001"
    assert valid_rows[1]["order_id"] == "ORD0003"


def test_13_empty_file(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "13_empty_file", "orders_13.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _orders_config()))

    assert exc.value.context["error_code"] == "NO_HEADER_ROW"


def test_14_header_only_no_rows_yields_zero_chunks(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "14_header_only_no_rows", "orders_14.csv")

    assert list(process_chunks(csv_path, _orders_config())) == []


def test_15_no_header_row(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "15_no_header_row", "orders_15.csv")

    # 03-03-PLAN.md's own <behavior>: the exact error_code depends on
    # detect_header's scoring heuristic (NO_HEADER_ROW or
    # MISSING_REQUIRED_COLUMN are both acceptable) -- assert the exception
    # TYPE and whole-file rejection only, never one specific code here.
    with pytest.raises(StructuralValidationError):
        list(process_chunks(csv_path, _orders_config()))


def test_16_ragged_rows_and_blank_lines(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "16_ragged_rows_and_blank_lines", "orders_16.csv")

    chunks = list(process_chunks(csv_path, _orders_config()))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 2
    assert [row["order_id"] for row in valid_rows] == ["ORD0001", "ORD0004"]

    assert len(invalid_rows) == 4
    assert all(row["error_code"] == "WRONG_COLUMN_COUNT" for row in invalid_rows)
    assert [row["row_number"] for row in invalid_rows] == [2, 3, 4, 5]
    # Row 2 and row 5 are the blank lines -- structurally absent fields,
    # None rather than "" (D-05).
    assert invalid_rows[0]["order_id"] is None
    assert invalid_rows[3]["order_id"] is None
    # Row 3 is the short row (2 of 4 fields) -- present fields keep their
    # real values, only the structurally-absent trailing fields are None.
    assert invalid_rows[1]["order_id"] == "ORD0002"
    assert invalid_rows[1]["customer_id"] == "CUST002"
    assert invalid_rows[1]["order_date"] is None
    assert invalid_rows[1]["amount"] is None
    # Row 4 is the long row (5 of 4 fields) -- every declared field is
    # present, the extra 5th field is simply dropped by the header-keyed
    # dict build, but the row is still flagged since raw field count (5)
    # != header field count (4).
    assert invalid_rows[2]["order_id"] == "ORD0003"
    assert invalid_rows[2]["amount"] == "300.00"
