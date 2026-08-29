"""Non-DB ``csv_processor.engine.process()`` status paths (ENGINE-08) --
``FILE_NOT_FOUND``, ``CONFIGURATION_ERROR``, ``INVALID_FILE``, and
``PROCESSING_ERROR``, every one proven WITHOUT a real Oracle connection
(mocked/patched per 04-RESEARCH.md's Wave 0 gap note). The DB-dependent
status paths (``SUCCESS``, ``SUCCESS_WITH_INVALID_ROWS``,
``DATABASE_ERROR``, the D-01 idempotency round trip) are proven separately
against the real container in ``tests/integration/test_engine_process_oracle.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process
from csv_processor.models import Status

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"

_WELL_FORMED_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
)

# Header missing the required `customer_id` column -> MISSING_REQUIRED_COLUMN,
# a whole-file StructuralValidationError reject (D-16..D-20) raised from
# source.prepare_source() before any row is processed.
_MISSING_REQUIRED_COLUMN_CSV = (
    "name,country,birth_date,event_ts,signup_country\nAlice Smith,DE,1990-01-01,"
    "2026-01-01T00:00:00+0000,FR\n"
)


def _load_customers_config() -> DatasetConfig:
    return load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)


def test_missing_file_returns_file_not_found(tmp_path: Path) -> None:
    config = _load_customers_config()
    missing_path = tmp_path / "does_not_exist.csv"

    with patch("csv_processor.engine.load.get_connection") as mock_get_connection:
        result = process(missing_path, config)

    mock_get_connection.assert_not_called()
    assert result.status == Status.FILE_NOT_FOUND
    assert result.total_rows == 0
    assert result.valid_rows == 0
    assert result.invalid_rows == 0
    assert result.duration_seconds > 0.0
    assert result.checksum is None


def test_invalid_config_returns_configuration_error(tmp_path: Path) -> None:
    config = _load_customers_config()
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_WELL_FORMED_CSV, encoding="utf-8")

    # A genuine pydantic.ValidationError instance (empty dict is missing every
    # required field) -- reused as a mock side_effect below rather than
    # calling the patched DatasetConfig.model_validate recursively.
    try:
        DatasetConfig.model_validate({})
    except ValidationError as exc:
        validation_error = exc

    with (
        patch("csv_processor.engine.load.get_connection") as mock_get_connection,
        patch(
            "csv_processor.engine.DatasetConfig.model_validate",
            side_effect=validation_error,
        ),
    ):
        result = process(csv_path, config)

    mock_get_connection.assert_not_called()
    assert result.status == Status.CONFIGURATION_ERROR
    assert result.total_rows == 0
    assert result.valid_rows == 0
    assert result.invalid_rows == 0
    assert result.duration_seconds > 0.0
    assert result.checksum is None


def test_structurally_broken_file_returns_invalid_file(tmp_path: Path) -> None:
    config = _load_customers_config()
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_MISSING_REQUIRED_COLUMN_CSV, encoding="utf-8")

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None  # find_existing_ingestion: no prior record
    mock_connection = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    with patch(
        "csv_processor.engine.load.get_connection", return_value=mock_connection
    ) as mock_get_connection:
        result = process(csv_path, config)

    mock_get_connection.assert_called_once()
    assert result.status == Status.INVALID_FILE
    assert result.total_rows == 0
    assert result.valid_rows == 0
    assert result.invalid_rows == 0
    assert result.duration_seconds > 0.0
    assert result.checksum is not None
    assert len(result.checksum) == 64


def test_unexpected_exception_returns_processing_error(tmp_path: Path) -> None:
    config = _load_customers_config()
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_WELL_FORMED_CSV, encoding="utf-8")

    with patch(
        "csv_processor.engine.load.get_connection", side_effect=RuntimeError("boom")
    ) as mock_get_connection:
        result = process(csv_path, config)

    mock_get_connection.assert_called_once()
    assert result.status == Status.PROCESSING_ERROR
    assert result.total_rows == 0
    assert result.valid_rows == 0
    assert result.invalid_rows == 0
    assert result.duration_seconds > 0.0
    assert result.checksum is not None
    assert len(result.checksum) == 64
