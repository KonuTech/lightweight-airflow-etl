"""Comprehensive Pydantic v2 validation-rule suite for csv_processor.config.models
(D-17, CONFIG-01/CONFIG-02 backing evidence).

Distinct from tests/unit/test_config_loader.py (which covers load_config()'s
success/failure/merge behavior end-to-end): this file proves every individual
Pydantic validation rule declared across ColumnSpec, CsvDialectConfig,
OracleTargetSpec, ProcessingConfig, and DatasetConfig, in isolation, by
constructing/validating model instances directly rather than round-tripping
through the file-loading path.

No new production code is introduced by this file -- every rule under test
already exists in packages/csv-processor/src/csv_processor/config/models.py
(Plan 01). Grouped into logical sections: type validation, precision/scale,
nullable/required combinations, CSV dialect extra-field round-trips,
delimiter/decimal-separator collision, extra="forbid" enforcement,
frozen-instance enforcement, and the credential-field-name mechanical scan
(T-02-02 backing evidence).
"""

from __future__ import annotations

import pytest
from csv_processor.config import (
    ColumnSpec,
    CsvDialectConfig,
    DatasetConfig,
    OracleTargetSpec,
    ProcessingConfig,
)
from pydantic import ValidationError

# A fully-specified valid dataset dict matching customers.json's shape.
_VALID_DATASET: dict = {
    "dataset": "customers",
    "file_pattern": "customers_*.csv",
    "csv": {
        "delimiter": ",",
        "encoding": "utf-8",
        "quotechar": '"',
        "header": True,
        "escapechar": None,
        "doublequote": True,
        "lineterminator": "\n",
        "decimal_separator": ".",
    },
    "columns": [
        {"name": "customer_id", "type": "string", "nullable": False, "required": True},
        {"name": "name", "type": "string", "nullable": False, "required": True},
        {"name": "country", "type": "string", "nullable": False, "required": True},
        {
            "name": "birth_date",
            "type": "date",
            "nullable": True,
            "required": True,
            "format": "%Y-%m-%d",
        },
        {
            "name": "event_ts",
            "type": "timestamp",
            "nullable": False,
            "required": True,
            "format": "%Y-%m-%dT%H:%M:%S%z",
        },
        {"name": "signup_country", "type": "string", "nullable": True, "required": False},
    ],
    "oracle": {"valid_table": "customers_valid", "invalid_table": "customers_invalid"},
    "processing": {"chunk_size": 5000},
}


def _minimal_column(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {"name": "col", "type": "string", "nullable": False, "required": True}
    base.update(overrides)
    return base


def _minimal_dataset(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "dataset": "x",
        "file_pattern": "x_*.csv",
        "columns": [_minimal_column()],
        "oracle": {"valid_table": "x_valid", "invalid_table": "x_invalid"},
        "processing": {"chunk_size": 1},
    }
    base.update(overrides)
    return base


# --- Full valid round-trip ---------------------------------------------------


class TestValidDatasetRoundTrip:
    def test_fully_specified_valid_dataset_round_trips(self) -> None:
        config = DatasetConfig.model_validate(_VALID_DATASET)

        assert config.dataset == "customers"
        assert len(config.columns) == 6
        assert config.oracle.valid_table == "customers_valid"
        assert config.processing.chunk_size == 5000


# --- Column type validation ---------------------------------------------------


class TestColumnTypeValidation:
    @pytest.mark.parametrize(
        "column_type",
        ["string", "integer", "decimal", "date", "timestamp", "boolean"],
    )
    def test_each_declared_column_type_is_accepted(self, column_type: str) -> None:
        extra: dict = {}
        if column_type in ("date", "timestamp"):
            extra["format"] = "%Y-%m-%d"
        if column_type == "decimal":
            extra["precision"] = 10
            extra["scale"] = 2

        column = ColumnSpec.model_validate(
            _minimal_column(type=column_type, nullable=True, **extra)
        )

        assert column.type == column_type

    def test_unrecognized_column_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(_minimal_column(type="varchar"))


# --- Precision/scale (decimal) -------------------------------------------------


class TestDecimalPrecisionScale:
    def test_decimal_with_valid_precision_and_scale_validates(self) -> None:
        column = ColumnSpec.model_validate(
            _minimal_column(type="decimal", nullable=True, precision=12, scale=2)
        )

        assert column.precision == 12
        assert column.scale == 2

    def test_decimal_scale_greater_than_precision_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(
                _minimal_column(type="decimal", nullable=True, precision=2, scale=5)
            )

    def test_decimal_missing_precision_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(_minimal_column(type="decimal", nullable=True, scale=2))

    def test_decimal_missing_scale_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(_minimal_column(type="decimal", nullable=True, precision=12))

    def test_decimal_missing_both_precision_and_scale_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(_minimal_column(type="decimal", nullable=True))


# --- Date/timestamp format requirement -----------------------------------------


class TestDateTimestampFormatRequirement:
    @pytest.mark.parametrize("column_type", ["date", "timestamp"])
    def test_date_or_timestamp_without_format_rejected(self, column_type: str) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(_minimal_column(type=column_type, nullable=True, format=None))

    @pytest.mark.parametrize("column_type", ["date", "timestamp"])
    def test_date_or_timestamp_with_format_validates(self, column_type: str) -> None:
        column = ColumnSpec.model_validate(
            _minimal_column(type=column_type, nullable=True, format="%Y-%m-%d")
        )

        assert column.format == "%Y-%m-%d"


# --- nullable/required independence (D-09) -------------------------------------


class TestNullableRequiredCombinations:
    @pytest.mark.parametrize(
        ("nullable", "required"),
        [(False, False), (False, True), (True, False), (True, True)],
    )
    def test_all_four_boolean_combinations_accepted_independently(
        self, nullable: bool, required: bool
    ) -> None:
        column = ColumnSpec.model_validate(_minimal_column(nullable=nullable, required=required))

        assert column.nullable is nullable
        assert column.required is required


# --- CSV dialect extra-field round-trips (D-02) --------------------------------


class TestCsvDialectExtraFields:
    def test_escapechar_and_doublequote_round_trip_with_non_default_values(self) -> None:
        dialect = CsvDialectConfig.model_validate({"escapechar": "\\", "doublequote": False})

        assert dialect.escapechar == "\\"
        assert dialect.doublequote is False

    def test_lineterminator_round_trips_with_non_default_value(self) -> None:
        dialect = CsvDialectConfig.model_validate({"lineterminator": "\r\n"})

        assert dialect.lineterminator == "\r\n"

    def test_decimal_separator_round_trips_with_non_default_value(self) -> None:
        dialect = CsvDialectConfig.model_validate({"decimal_separator": ","})

        assert dialect.decimal_separator == ","

    def test_has_footer_defaults_to_false(self) -> None:
        """FTR-01: absent from every dataset config shipped today (customers.json/
        orders.json both omit the `csv` block entirely) -- must default to False so
        footer-shape exclusion is never applied unless a dataset explicitly opts in."""
        dialect = CsvDialectConfig.model_validate({})

        assert dialect.has_footer is False

    def test_has_footer_round_trips_with_explicit_true(self) -> None:
        dialect = CsvDialectConfig.model_validate({"has_footer": True})

        assert dialect.has_footer is True


# --- delimiter/decimal_separator collision -------------------------------------


class TestDelimiterDecimalSeparatorCollision:
    @pytest.mark.parametrize("shared_char", [",", ";"])
    def test_delimiter_equal_to_decimal_separator_rejected(self, shared_char: str) -> None:
        dataset = _minimal_dataset(csv={"delimiter": shared_char, "decimal_separator": shared_char})

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)

    def test_delimiter_different_from_decimal_separator_validates(self) -> None:
        dataset = _minimal_dataset(csv={"delimiter": ";", "decimal_separator": ","})

        config = DatasetConfig.model_validate(dataset)

        assert config.csv.delimiter == ";"
        assert config.csv.decimal_separator == ","


# --- extra="forbid" enforcement -------------------------------------------------


class TestExtraForbidEnforcement:
    def test_unrecognized_top_level_key_rejected(self) -> None:
        dataset = _minimal_dataset(not_a_real_field=True)

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)

    def test_unrecognized_key_nested_in_csv_rejected(self) -> None:
        dataset = _minimal_dataset(csv={"not_a_real_field": True})

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)

    def test_unrecognized_key_nested_in_oracle_rejected(self) -> None:
        dataset = _minimal_dataset(
            oracle={
                "valid_table": "x_valid",
                "invalid_table": "x_invalid",
                "connection_string": "should-never-exist",
            }
        )

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)

    def test_unrecognized_key_nested_in_processing_rejected(self) -> None:
        dataset = _minimal_dataset(processing={"chunk_size": 1, "not_a_real_field": True})

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)

    def test_unrecognized_key_nested_in_column_rejected(self) -> None:
        dataset = _minimal_dataset(columns=[_minimal_column(not_a_real_field="should-never-exist")])

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)


# --- empty columns list ---------------------------------------------------------


class TestEmptyColumnsRejected:
    def test_empty_columns_list_rejected(self) -> None:
        dataset = _minimal_dataset(columns=[])

        with pytest.raises(ValidationError):
            DatasetConfig.model_validate(dataset)


# --- frozen-instance enforcement -------------------------------------------------


class TestFrozenInstanceEnforcement:
    def test_mutating_dataset_config_after_construction_rejected(self) -> None:
        config = DatasetConfig.model_validate(_minimal_dataset())

        with pytest.raises(ValidationError):
            config.dataset = "other"  # type: ignore[misc]

    def test_mutating_column_spec_after_construction_rejected(self) -> None:
        column = ColumnSpec.model_validate(_minimal_column())

        with pytest.raises(ValidationError):
            column.name = "other"  # type: ignore[misc]

    def test_mutating_csv_dialect_config_after_construction_rejected(self) -> None:
        dialect = CsvDialectConfig.model_validate({})

        with pytest.raises(ValidationError):
            dialect.delimiter = ";"  # type: ignore[misc]

    def test_mutating_oracle_target_spec_after_construction_rejected(self) -> None:
        oracle = OracleTargetSpec.model_validate(
            {"valid_table": "x_valid", "invalid_table": "x_invalid"}
        )

        with pytest.raises(ValidationError):
            oracle.valid_table = "other"  # type: ignore[misc]

    def test_mutating_processing_config_after_construction_rejected(self) -> None:
        processing = ProcessingConfig.model_validate({"chunk_size": 1})

        with pytest.raises(ValidationError):
            processing.chunk_size = 2  # type: ignore[misc]


# --- SQL identifier allowlist (T-04-01) -----------------------------------------


class TestSqlIdentifierAllowlist:
    def test_column_name_rejects_unsafe_identifier(self) -> None:
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(_minimal_column(name="1bad; DROP TABLE"))

    def test_oracle_target_spec_rejects_unsafe_table_name(self) -> None:
        with pytest.raises(ValidationError):
            OracleTargetSpec.model_validate({"valid_table": "ok_valid", "invalid_table": "bad; --"})


# --- credential-field-name mechanical scan (T-02-02, privacy prohibition) -------


class TestNoCredentialFieldNames:
    """Mechanical proof that the schema structurally cannot carry a credential
    field: iterates every field name declared across every model class and
    asserts none of them, case-insensitively, contains a credential-shaped
    substring. Backs 02-01-PLAN.md's must_haves.prohibitions entry (D-12)."""

    _FORBIDDEN_SUBSTRINGS = (
        "password",
        "secret",
        "credential",
        "connection",
        "conn_str",
        "dsn",
    )

    _MODEL_CLASSES = (
        ColumnSpec,
        CsvDialectConfig,
        OracleTargetSpec,
        ProcessingConfig,
        DatasetConfig,
    )

    def test_no_model_field_name_resembles_a_credential(self) -> None:
        offending: list[str] = []
        for model_class in self._MODEL_CLASSES:
            for field_name in model_class.model_fields:
                lowered = field_name.lower()
                for forbidden in self._FORBIDDEN_SUBSTRINGS:
                    if forbidden in lowered:
                        offending.append(f"{model_class.__name__}.{field_name}")

        assert offending == []
