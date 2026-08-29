"""Row-level nullability/type validation (ENGINE-02/ENGINE-03) with a
deterministic check-priority/tie-break (D-13/D-14/D-15).

Structural short-circuit (D-13 -- a row whose field count doesn't match the
header) already happened in ``engine.py`` before this module is ever
called; ``check_row``/``normalize_row`` only ever see a row that is
structurally sound. This is a fresh design with no reference-repo file
precedent for the check-priority/tie-break logic itself (03-PATTERNS.md's
"No Analog Found" table) -- only the individual per-type conversion
algorithms it delegates to (``normalize.convert_value``) have Tier-B
precedent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from csv_processor import errors, normalize

if TYPE_CHECKING:
    from csv_processor.config.models import ColumnSpec, DatasetConfig


def check_row(
    row: dict[str, str], config: DatasetConfig
) -> tuple[str | None, str | None, str | None]:
    """Run every nullability check, then every type check, across the row.

    Never stops at the first violation found (D-13's exhaustive-once-
    structurally-sound rule) -- every column is checked so the reported
    violation is the correct priority winner, not an artifact of iteration
    order alone.

    Args:
        row: One structurally-sound row, keyed by column name -> original
            string value (never ``None`` -- a genuinely absent field is a
            structural failure ``engine.py`` already handled before this
            function is called).
        config: The dataset's validated config -- ``config.columns``'s
            declared order drives both the check loop and the D-15
            tie-break.

    Returns:
        ``(error_code, error_message, error_column)`` for the
        highest-priority violation found (nullability beats type, D-14;
        the first declared column wins a same-kind tie, D-15), or
        ``(None, None, None)`` when the row is fully valid.
    """
    nullability_violations: list[ColumnSpec] = []
    type_violations: list[tuple[ColumnSpec, str]] = []

    for column in config.columns:
        value = row[column.name]
        if not column.nullable and value == "":
            # A null violation makes type-checking this column meaningless
            # (D-14) -- never call convert_value on it.
            nullability_violations.append(column)
            continue
        if column.nullable and value == "":
            # Empty + nullable is valid, never a type-check target (D-10's
            # own short-circuit, mirrored here).
            continue
        _typed_value, err = normalize.convert_value(value, column)
        if err:
            type_violations.append((column, err))

    if nullability_violations:
        column = nullability_violations[0]  # first in declared order, D-15
        return (
            errors.NULL_VIOLATION,
            f"{column.name!r} is required but empty",
            column.name,
        )
    if type_violations:
        column, err = type_violations[0]  # first in declared order, D-15
        return err, f"{column.name!r} failed type check: {err}", column.name
    return None, None, None


def normalize_row(row: dict[str, str], config: DatasetConfig) -> dict[str, object]:
    """Convert every field in a known-valid row to its typed Python value.

    Only ever called on a row ``check_row`` already returned
    ``(None, None, None)`` for -- this function assumes no error and does
    not re-check.

    Args:
        row: One structurally-sound, already-validated row, keyed by
            column name -> original string value.
        config: The dataset's validated config.

    Returns:
        The same row, keyed by column name -> typed Python value
        (``datetime.date``, ``datetime.datetime``, ``Decimal``, ``int``,
        ``str``, ``bool``, or ``None`` for an empty nullable field, D-10) --
        never carries any ``error_*`` key.
    """
    out: dict[str, object] = {}
    for column in config.columns:
        value = row[column.name]
        if column.nullable and value == "":
            # D-10 -- never calls convert_value on an empty nullable field;
            # its typed value is Python None, never an empty string.
            out[column.name] = None
            continue
        typed_value, _err = normalize.convert_value(value, column)
        out[column.name] = typed_value
    return out
