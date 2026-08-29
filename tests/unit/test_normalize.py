"""Proves ``csv_processor.normalize.convert_value``'s complete type-dispatch
table (03-03-PLAN.md Task 2) -- every one of the six declared column types
(``string``/``integer``/``decimal``/``date``/``timestamp``/``boolean``),
both a passing and a failing case each, plus dedicated boundary tests for
``parse_decimal_strict``/``parse_date_strict`` at their exact declared
limits (Pattern 3/Pattern 4, 03-RESEARCH.md).
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from csv_processor.config.models import ColumnSpec
from csv_processor.normalize import convert_value, parse_date_strict, parse_decimal_strict


def _column(**overrides: object) -> ColumnSpec:
    defaults: dict[str, object] = {
        "name": "col",
        "type": "string",
        "nullable": False,
        "required": True,
    }
    defaults.update(overrides)
    return ColumnSpec(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# convert_value -- one section per column type, success + failure each.
# ---------------------------------------------------------------------------


def test_string_always_passes_through_unchanged() -> None:
    column = _column(type="string")
    assert convert_value("hello world", column) == ("hello world", None)
    assert convert_value("", column) == ("", None)


def test_integer_success() -> None:
    column = _column(type="integer")
    assert convert_value("42", column) == (42, None)


def test_integer_failure_not_a_number() -> None:
    column = _column(type="integer")
    value, error_code = convert_value("not-a-number", column)
    assert value is None
    assert error_code == "TYPE_MISMATCH"


def test_decimal_success_within_precision_and_scale() -> None:
    column = _column(type="decimal", precision=12, scale=2)
    value, error_code = convert_value("100.99", column)
    assert value == Decimal("100.99")
    assert error_code is None


def test_decimal_failure_not_a_number() -> None:
    column = _column(type="decimal", precision=12, scale=2)
    value, error_code = convert_value("not-a-decimal", column)
    assert value is None
    assert error_code == "TYPE_MISMATCH"


def test_decimal_failure_scale_exceeded() -> None:
    column = _column(type="decimal", precision=12, scale=2)
    value, error_code = convert_value("100.999", column)
    assert value is None
    assert error_code == "DECIMAL_PRECISION_EXCEEDED"


def test_date_success() -> None:
    column = _column(type="date", format="%Y-%m-%d")
    value, error_code = convert_value("2026-01-01", column)
    assert value == dt.date(2026, 1, 1)
    assert error_code is None


def test_date_failure_invalid_format() -> None:
    column = _column(type="date", format="%Y-%m-%d")
    value, error_code = convert_value("31/02/2026", column)
    assert value is None
    assert error_code == "INVALID_DATE_FORMAT"


def test_timestamp_success() -> None:
    column = _column(type="timestamp", format="%Y-%m-%dT%H:%M:%S%z")
    value, error_code = convert_value("2026-01-01T00:00:00+0000", column)
    assert value == dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    assert error_code is None


def test_timestamp_failure_invalid_format() -> None:
    column = _column(type="timestamp", format="%Y-%m-%dT%H:%M:%S%z")
    value, error_code = convert_value("2026-01-01 00:00:00", column)
    assert value is None
    assert error_code == "INVALID_TIMESTAMP_FORMAT"


def test_boolean_success_true_values() -> None:
    column = _column(type="boolean")
    assert convert_value("true", column) == (True, None)
    assert convert_value("1", column) == (True, None)
    assert convert_value("TRUE", column) == (True, None)


def test_boolean_success_false_values() -> None:
    column = _column(type="boolean")
    assert convert_value("false", column) == (False, None)
    assert convert_value("0", column) == (False, None)


def test_boolean_failure_unrecognized_value() -> None:
    column = _column(type="boolean")
    value, error_code = convert_value("maybe", column)
    assert value is None
    assert error_code == "TYPE_MISMATCH"


# ---------------------------------------------------------------------------
# parse_decimal_strict -- exact declared boundary.
# ---------------------------------------------------------------------------


def test_parse_decimal_strict_scale_exactly_equal_to_declared_passes() -> None:
    value, error_code = parse_decimal_strict("100.99", precision=12, scale=2)
    assert value == Decimal("100.99")
    assert error_code is None


def test_parse_decimal_strict_scale_one_more_than_declared_fails() -> None:
    value, error_code = parse_decimal_strict("100.999", precision=12, scale=2)
    assert value is None
    assert error_code == "DECIMAL_PRECISION_EXCEEDED"


def test_parse_decimal_strict_precision_exactly_equal_to_declared_passes() -> None:
    # 12 significant digits, scale 2 -> 10 integer digits + 2 fractional.
    value, error_code = parse_decimal_strict("1234567890.12", precision=12, scale=2)
    assert value == Decimal("1234567890.12")
    assert error_code is None


def test_parse_decimal_strict_precision_one_more_than_declared_fails() -> None:
    value, error_code = parse_decimal_strict("12345678901.12", precision=12, scale=2)
    assert value is None
    assert error_code == "DECIMAL_PRECISION_EXCEEDED"


def test_parse_decimal_strict_integer_valued_decimal_has_zero_scale() -> None:
    # No '.' at all -- exercises the exponent >= 0 branch, never breaks on a
    # naive string-length heuristic (03-RESEARCH.md Pitfall 4).
    value, error_code = parse_decimal_strict("100", precision=12, scale=2)
    assert value == Decimal("100")
    assert error_code is None


def test_parse_decimal_strict_not_a_number_is_type_mismatch() -> None:
    value, error_code = parse_decimal_strict("abc", precision=12, scale=2)
    assert value is None
    assert error_code == "TYPE_MISMATCH"


def test_parse_decimal_strict_nan_is_type_mismatch_not_precision_exceeded() -> None:
    # Decimal("nan") parses successfully but is never a legitimate monetary
    # value -- its as_tuple().exponent is a special string, not an int.
    value, error_code = parse_decimal_strict("nan", precision=12, scale=2)
    assert value is None
    assert error_code == "TYPE_MISMATCH"


# ---------------------------------------------------------------------------
# parse_date_strict -- exact strptime match vs. one-character-off format.
# ---------------------------------------------------------------------------


def test_parse_date_strict_valid_match_passes() -> None:
    value, error_code = parse_date_strict("2026-01-01", "%Y-%m-%d", is_timestamp=False)
    assert value == dt.date(2026, 1, 1)
    assert error_code is None


def test_parse_date_strict_one_character_off_format_fails() -> None:
    # Slash-separated value against a dash-separated format.
    value, error_code = parse_date_strict("2026/01/01", "%Y-%m-%d", is_timestamp=False)
    assert value is None
    assert error_code == "INVALID_DATE_FORMAT"


def test_parse_date_strict_timestamp_valid_match_passes() -> None:
    value, error_code = parse_date_strict(
        "2026-01-01T00:00:00+0000", "%Y-%m-%dT%H:%M:%S%z", is_timestamp=True
    )
    assert value == dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    assert error_code is None


def test_parse_date_strict_timestamp_one_character_off_format_fails() -> None:
    value, error_code = parse_date_strict(
        "2026-01-01 00:00:00+0000", "%Y-%m-%dT%H:%M:%S%z", is_timestamp=True
    )
    assert value is None
    assert error_code == "INVALID_TIMESTAMP_FORMAT"
