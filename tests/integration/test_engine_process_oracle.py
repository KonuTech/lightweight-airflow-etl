"""Real-Oracle integration proof for ``csv_processor.engine.process()``
(ENGINE-08, TEST-02) -- every test in this file runs against the actually
running Oracle Database Free container (``make up``), never a mock.

This file proves the DB-dependent status paths end to end: ``SUCCESS``,
``SUCCESS_WITH_INVALID_ROWS``, ``DATABASE_ERROR``, and the D-01 re-process
idempotency round trip. The non-DB status paths (``FILE_NOT_FOUND``,
``CONFIGURATION_ERROR``, ``INVALID_FILE``, ``PROCESSING_ERROR``) are proven
separately, mocked, in ``tests/unit/test_engine_process.py`` (Task 2).
"""

from __future__ import annotations

from pathlib import Path

import oracledb

from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process
from csv_processor.models import Status

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"

_ALL_VALID_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
    "CUST002,Bob Jones,US,1985-05-05,2026-01-02T00:00:00+0000,US\n"
    "CUST003,Carol White,GB,1975-12-12,2026-01-03T00:00:00+0000,GB\n"
)

# Two valid rows + one invalid row (empty required customer_id -> NULL_VIOLATION).
_MIXED_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
    "CUST002,Bob Jones,US,1985-05-05,2026-01-02T00:00:00+0000,US\n"
    ",Carol White,GB,1975-12-12,2026-01-03T00:00:00+0000,GB\n"
)

# One row whose customer_id is 65 characters -- customers_invalid.customer_id
# is VARCHAR2(64). Blank required `name` makes this row fail Phase 3's
# NULL_VIOLATION check first (a genuine type/nullability PASS-then-invalid
# row, not a structural reject), so it reaches load.insert_rows() destined
# for customers_invalid, where Oracle itself rejects the oversized value.
_OVERSIZED_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    f"{'X' * 65},,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
)


def _load_customers_config() -> DatasetConfig:
    return load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)


def test_process_success_status_end_to_end(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_20260829_success.csv"
    csv_path.write_text(_ALL_VALID_CSV, encoding="utf-8")
    config = _load_customers_config()

    result = process(csv_path, config)

    assert result.status == Status.SUCCESS
    assert result.total_rows == 3
    assert result.valid_rows == 3
    assert result.invalid_rows == 0
    assert result.checksum is not None
    assert len(result.checksum) == 64
    assert all(c in "0123456789abcdef" for c in result.checksum)
    assert result.duration_seconds > 0.0

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM customers_valid WHERE customer_id IN "
        "('CUST001', 'CUST002', 'CUST003')"
    )
    (count,) = oracle_cursor.fetchone()
    assert count == 3


def test_process_success_with_invalid_rows(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_20260829_mixed.csv"
    csv_path.write_text(_MIXED_CSV, encoding="utf-8")
    config = _load_customers_config()

    result = process(csv_path, config)

    assert result.status == Status.SUCCESS_WITH_INVALID_ROWS
    assert result.total_rows == 3
    assert result.valid_rows == 2
    assert result.invalid_rows == 1

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM customers_valid WHERE customer_id IN ('CUST001', 'CUST002')"
    )
    (valid_count,) = oracle_cursor.fetchone()
    assert valid_count == 2

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM customers_invalid WHERE source_file = :source_file",
        {"source_file": csv_path.name},
    )
    (invalid_count,) = oracle_cursor.fetchone()
    assert invalid_count == 1


def test_process_oversized_value_returns_database_error(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_20260829_oversized.csv"
    csv_path.write_text(_OVERSIZED_CSV, encoding="utf-8")
    config = _load_customers_config()

    result = process(csv_path, config)

    assert result.status == Status.DATABASE_ERROR

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM customers_invalid WHERE source_file = :source_file",
        {"source_file": csv_path.name},
    )
    (invalid_count,) = oracle_cursor.fetchone()
    assert invalid_count == 0

    oracle_cursor.execute(
        "SELECT COUNT(*) FROM ingestion_metadata WHERE dataset = 'customers' "
        "AND file_name = :file_name",
        {"file_name": csv_path.name},
    )
    (metadata_count,) = oracle_cursor.fetchone()
    assert metadata_count == 0


def test_process_reprocess_returns_identical_result(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_20260829_reprocess.csv"
    csv_path.write_text(_ALL_VALID_CSV, encoding="utf-8")
    config = _load_customers_config()

    first_result = process(csv_path, config)
    assert first_result.status == Status.SUCCESS

    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (valid_count_after_first,) = oracle_cursor.fetchone()
    oracle_cursor.execute("SELECT COUNT(*) FROM customers_invalid")
    (invalid_count_after_first,) = oracle_cursor.fetchone()
    oracle_cursor.execute(
        "SELECT COUNT(*) FROM ingestion_metadata WHERE dataset = :dataset AND checksum = :checksum",
        {"dataset": "customers", "checksum": first_result.checksum},
    )
    (metadata_count_after_first,) = oracle_cursor.fetchone()
    assert metadata_count_after_first == 1

    second_result = process(csv_path, config)

    assert second_result == first_result

    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (valid_count_after_second,) = oracle_cursor.fetchone()
    oracle_cursor.execute("SELECT COUNT(*) FROM customers_invalid")
    (invalid_count_after_second,) = oracle_cursor.fetchone()
    oracle_cursor.execute(
        "SELECT COUNT(*) FROM ingestion_metadata WHERE dataset = :dataset AND checksum = :checksum",
        {"dataset": "customers", "checksum": second_result.checksum},
    )
    (metadata_count_after_second,) = oracle_cursor.fetchone()

    assert valid_count_after_second == valid_count_after_first
    assert invalid_count_after_second == invalid_count_after_first
    assert metadata_count_after_second == 1
