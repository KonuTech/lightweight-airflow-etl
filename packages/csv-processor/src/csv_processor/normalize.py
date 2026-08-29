"""Per-type string -> Python value conversion (ENGINE-04).

Two of the six branches below (``parse_decimal_strict``/``parse_date_strict``)
are the Tier-B derived/adapted helpers 03-RESEARCH.md Patterns 3/4 verified
live against this project's own real config values and corpus fixtures --
see 03-PATTERNS.md's "No Analog Found" table for exactly what has no
reference-repo precedent (the decimal precision/scale check) versus what
was adapted from a reference algorithm (the strict ``strptime`` rejection,
from ``dataplat/normalize/dates.py``, stripped of all
``StreamingStage``/DST/pivot-year/spreadsheet-serial machinery this
project's ``ColumnSpec`` has no fields to drive).

Every column type has an explicit conversion path; a value that fails its
declared type/precision/scale check is always rejected via an
``error_code`` string, never silently truncated/rounded/coerced into a
"close enough" typed value (03-03-PLAN.md's own prohibitions list) --
``float()`` is never used anywhere in this module for decimal parsing
(T-03-05's mitigation; ``dataplat/normalize/numeric.py``'s own
unconditional rule, "this module never converts a parsed value through
``float``").
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from csv_processor import errors

if TYPE_CHECKING:
    from csv_processor.config.models import ColumnSpec

_TRUE_VALUES = ("true", "1")
_FALSE_VALUES = ("false", "0")


def parse_decimal_strict(
    raw: str, *, precision: int, scale: int
) -> tuple[Decimal | None, str | None]:
    """Parse ``raw`` as an exact ``Decimal`` and verify precision/scale.

    Uses ``Decimal.as_tuple()``'s own ``(sign, digits, exponent)``
    representation to derive the value's actual scale/precision -- never a
    string-length heuristic, which breaks on trailing zeros stripped,
    leading zeros, or integer-valued decimals with no ``.`` at all
    (03-RESEARCH.md Pitfall 4).

    Args:
        raw: The original CSV field text.
        precision: The column's declared total significant digits.
        scale: The column's declared digits after the decimal point.

    Returns:
        ``(value, None)`` on success, or ``(None, error_code)`` -- either
        ``errors.TYPE_MISMATCH`` (not a valid decimal at all, or a
        non-finite special value like NaN/Infinity) or
        ``errors.DECIMAL_PRECISION_EXCEEDED`` (parses fine but exceeds the
        declared precision/scale).
    """
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None, errors.TYPE_MISMATCH

    _sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        # Decimal.as_tuple()'s exponent is the special string "n"/"N"/"F"
        # for NaN/sNaN/Infinity -- all parse successfully but are never a
        # legitimate monetary value; reject as a type mismatch rather than
        # attempting scale/precision arithmetic against a non-numeric
        # exponent.
        return None, errors.TYPE_MISMATCH

    if exponent >= 0:
        value_scale, value_precision = 0, len(digits) + exponent
    else:
        value_scale, value_precision = -exponent, max(len(digits), -exponent)

    if value_scale > scale or value_precision > precision:
        return None, errors.DECIMAL_PRECISION_EXCEEDED
    return value, None


def parse_date_strict(
    raw: str, fmt: str, *, is_timestamp: bool
) -> tuple[dt.date | dt.datetime | None, str | None]:
    """Parse ``raw`` with a strict, single-format ``strptime`` -- never a
    format-guessing fallback (``dateutil.parser.parse`` is explicitly
    excluded, STACK.md).

    Args:
        raw: The original CSV field text.
        fmt: The column's declared ``strptime`` format string.
        is_timestamp: Whether the column is a ``timestamp`` (returns a
            ``datetime.datetime``) or a ``date`` (returns a
            ``datetime.date``, truncating any time-of-day component
            ``strptime`` may have parsed).

    Returns:
        ``(value, None)`` on success, or ``(None, error_code)`` where
        ``error_code`` is ``errors.INVALID_TIMESTAMP_FORMAT`` or
        ``errors.INVALID_DATE_FORMAT`` depending on ``is_timestamp``.
    """
    try:
        parsed = dt.datetime.strptime(raw, fmt)
    except ValueError:
        code = errors.INVALID_TIMESTAMP_FORMAT if is_timestamp else errors.INVALID_DATE_FORMAT
        return None, code
    return (parsed if is_timestamp else parsed.date()), None


def convert_value(raw: str, column: ColumnSpec) -> tuple[object, str | None]:
    """Convert one CSV field's string value to its declared Python type.

    Dispatches on ``column.type`` -- every one of the six declared column
    types (D-11) has an explicit branch, never a silent fallthrough.

    Args:
        raw: The original CSV field text (never pre-stripped/normalized by
            the caller -- this function's own per-type logic decides what,
            if anything, to normalize).
        column: The column's full type/format/precision/scale contract.

    Returns:
        ``(value, None)`` on success (``value`` typed per ``column.type``:
        ``str``, ``int``, ``Decimal``, ``datetime.date``,
        ``datetime.datetime``, or ``bool``), or ``(None, error_code)`` on
        failure -- never raises for a malformed value; every failure path
        is an explicit rejection, matching this module's own "never guess"
        contract.
    """
    if column.type == "string":
        return raw, None

    if column.type == "integer":
        try:
            return int(raw), None
        except ValueError:
            return None, errors.TYPE_MISMATCH

    if column.type == "decimal":
        assert column.precision is not None
        assert column.scale is not None
        return parse_decimal_strict(raw, precision=column.precision, scale=column.scale)

    if column.type == "date":
        assert column.format is not None
        return parse_date_strict(raw, column.format, is_timestamp=False)

    if column.type == "timestamp":
        assert column.format is not None
        return parse_date_strict(raw, column.format, is_timestamp=True)

    if column.type == "boolean":
        normalized = raw.strip().lower()
        if normalized in _TRUE_VALUES:
            return True, None
        if normalized in _FALSE_VALUES:
            return False, None
        return None, errors.TYPE_MISMATCH

    msg = f"unknown column type {column.type!r} for column {column.name!r}"
    raise ValueError(msg)
