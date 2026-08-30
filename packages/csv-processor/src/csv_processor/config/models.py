"""Pydantic v2 config-contract model tree for a dataset's ingestion config
(CONFIG-01/CONFIG-02).

Every model is frozen and rejects unrecognized keys via each class's own
``model_config`` -- ported convention from the reference repo's
``dataplat.config.model`` (verified identical across all 13 of its model classes,
read directly during this phase's research). A validated ``DatasetConfig`` instance
can never be mutated after construction, and an unrecognized/typo'd config key is a
validation-time error rather than a silently-ignored one (T-02-01).

Column shape (``ColumnSpec``) and the ``customers``/``orders`` datasets' actual
values are verified against this project's own Oracle DDL
(``docker/oracle/init/02_customers.sql``, ``03_orders.sql``), not the reference
repo's illustrative ``.yaml`` files -- see 02-RESEARCH.md's "Config Model Shape".
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_COLUMN_TYPES = Literal["string", "integer", "decimal", "date", "timestamp", "boolean"]  # D-11

# T-04-01: Oracle has no bind-parameter mechanism for SQL identifiers (table/column
# names) -- only VALUES bind safely. Every dynamically-built INSERT statement in
# this project (csv_processor.load) interpolates config-sourced identifiers
# directly into the SQL string, so this allowlist is the only defense available
# against a malformed/malicious identifier reaching a live SQL statement. Applied
# at TWO layers: here (config-load time, below) and again in load.insert_rows()
# immediately before building the INSERT string (defense-in-depth).
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def is_safe_identifier(name: str) -> bool:
    """Return True iff ``name`` is safe to interpolate as a bare SQL identifier.

    Matches ``^[A-Za-z_][A-Za-z0-9_]{0,127}$`` -- a leading letter/underscore,
    then up to 127 more letters/digits/underscores. Rejects anything shaped
    like a SQL-injection attempt (``"x; DROP TABLE"``), a numeric-leading name
    (``"1bad"``), or a comment marker (``"good--comment"``).
    """
    return bool(_IDENTIFIER_RE.match(name))


class ColumnSpec(BaseModel):
    """One column's type, nullability, and (for date/timestamp/decimal) format
    contract.

    ``nullable`` (can a present value be empty) and ``required`` (must the
    column be present in the file at all) are deliberately two separate
    booleans (D-09) -- explicit future-proofing, even though neither of this
    project's two fixed-schema datasets currently needs the distinction.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    type: _COLUMN_TYPES
    nullable: bool
    required: bool
    format: str | None = None  # strptime string (D-08) -- required for date/timestamp only
    precision: int | None = Field(default=None, gt=0)  # D-10 -- decimal only
    scale: int | None = Field(default=None, gt=0)  # D-10 -- decimal only

    @model_validator(mode="after")
    def _check_type_specific_fields(self) -> ColumnSpec:
        if self.type in ("date", "timestamp") and not self.format:
            msg = f"column {self.name!r}: type {self.type!r} requires a non-empty 'format'"
            raise ValueError(msg)
        if self.type == "decimal":
            if self.precision is None or self.scale is None:
                msg = f"column {self.name!r}: type 'decimal' requires both 'precision' and 'scale'"
                raise ValueError(msg)
            if self.scale > self.precision:
                msg = (
                    f"column {self.name!r}: scale ({self.scale}) cannot exceed "
                    f"precision ({self.precision})"
                )
                raise ValueError(msg)
        elif self.precision is not None or self.scale is not None:
            msg = f"column {self.name!r}: 'precision'/'scale' are only valid for type 'decimal'"
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _check_name_is_safe_sql_identifier(self) -> ColumnSpec:
        if not is_safe_identifier(self.name):
            msg = (
                f"column name {self.name!r} is not a safe SQL identifier "
                f"(must match {_IDENTIFIER_RE.pattern!r}) -- Oracle has no "
                "bind-parameter mechanism for identifiers, only values, and this "
                "name is interpolated directly into a dynamically-built INSERT "
                "statement (T-04-01)"
            )
            raise ValueError(msg)
        return self


class CsvDialectConfig(BaseModel):
    """CSV dialect fields, every one with a sane default (D-01/D-02/D-03) --
    every field always has a concrete value, unlike the reference repo's own
    detect-or-override shape (``str | None = None``).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    delimiter: str = ","
    encoding: str = "utf-8"
    quotechar: str = '"'
    header: bool = True
    escapechar: str | None = None  # D-02
    doublequote: bool = True  # D-02
    lineterminator: str = "\n"  # D-02
    decimal_separator: str = "."  # D-17 gap resolution, see 02-01-PLAN.md's planner_gap_note
    # FTR-01 gap resolution, see 03-VERIFICATION.md's new gap finding / 03-10-PLAN.md --
    # per-dataset opt-in for source.py's footer-shape exclusion heuristic; False (every
    # dataset config shipped today) means a genuinely malformed last row is NEVER
    # excluded on field-count-mismatch grounds alone
    has_footer: bool = False

    @model_validator(mode="after")
    def _check_escapechar_present_when_doublequote_disabled(self) -> CsvDialectConfig:
        if not self.doublequote and not self.escapechar:
            msg = (
                "csv.doublequote is false but csv.escapechar is unset; a field "
                "requiring escaping would crash at write/parse time with no way to represent it"
            )
            raise ValueError(msg)
        return self


class OracleTargetSpec(BaseModel):
    """Oracle target/invalid table names only -- never connection details or
    credentials (D-12). Keeps config.json safe to log or version.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    valid_table: str
    invalid_table: str

    @model_validator(mode="after")
    def _check_valid_and_invalid_tables_differ(self) -> OracleTargetSpec:
        if self.valid_table.lower() == self.invalid_table.lower():
            msg = (
                f"oracle.valid_table and oracle.invalid_table must differ, "
                f"both are {self.valid_table!r}"
            )
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _check_table_names_are_safe_sql_identifiers(self) -> OracleTargetSpec:
        for field_name, value in (
            ("valid_table", self.valid_table),
            ("invalid_table", self.invalid_table),
        ):
            if not is_safe_identifier(value):
                msg = (
                    f"oracle.{field_name} {value!r} is not a safe SQL identifier "
                    f"(must match {_IDENTIFIER_RE.pattern!r}) -- Oracle has no "
                    "bind-parameter mechanism for identifiers, only values, and this "
                    "name is interpolated directly into a dynamically-built INSERT "
                    "statement (T-04-01)"
                )
                raise ValueError(msg)
        return self


class ProcessingConfig(BaseModel):
    """Per-dataset processing knobs (D-13)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    chunk_size: int = Field(gt=0)


class DatasetConfig(BaseModel):
    """The complete, validated configuration for one dataset.

    Every ``configs/datasets/<name>.json``, merged over
    ``configs/defaults.json``, must validate against this model with zero
    errors (``csv_processor.config.loader.load_config``).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset: str
    file_pattern: str  # D-07: glob, e.g. "customers_*.csv"
    csv: CsvDialectConfig = Field(default_factory=CsvDialectConfig)
    columns: list[ColumnSpec] = Field(min_length=1)
    oracle: OracleTargetSpec
    processing: ProcessingConfig

    @model_validator(mode="after")
    def _check_delimiter_does_not_collide_with_decimal_separator(self) -> DatasetConfig:
        """Reject a delimiter that is also the decimal separator (D-17 gap
        resolution; mirrors ``dataplat/config/model.py:706-728``, scoped down to
        this project's flat ``csv`` block).

        Dialect parsing runs before any numeric interpretation, so such a
        declaration is unsatisfiable by construction: the parser would split a
        decimal number in half.
        """
        if self.csv.delimiter == self.csv.decimal_separator:
            msg = (
                f"csv.delimiter {self.csv.delimiter!r} is also "
                f"csv.decimal_separator {self.csv.decimal_separator!r}; no parser "
                "could ever read this file correctly"
            )
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _check_column_names_are_unique(self) -> DatasetConfig:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            dupes = sorted({n for n in names if names.count(n) > 1})
            msg = f"duplicate column name(s) in 'columns': {dupes}"
            raise ValueError(msg)
        return self
