"""Real-Oracle integration proof for ``csv_processor.load`` (LOAD-01 through
LOAD-04, TEST-02) -- every test in this file runs against the actually
running Oracle Database Free container (``make up``), never a mock.

``process()`` does not exist yet (that's Plan 04-02's job) -- this file
proves ``load.py``'s own mechanism directly: a tmp_path-authored CSV is run
through Phase 3's real ``process_chunks()`` to get real
``(valid_rows, invalid_rows)`` tuples, which are then fed straight into
``load.insert_rows``/``load.record_ingestion``.
"""

from __future__ import annotations

from pathlib import Path

import oracledb
import pytest

from csv_processor import load
from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process_chunks

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"

_ALL_VALID_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
    "CUST002,Bob Jones,US,1985-05-05,2026-01-02T00:00:00+0000,US\n"
    "CUST003,Carol White,GB,1975-12-12,2026-01-03T00:00:00+0000,GB\n"
)

# One valid + one invalid (empty required customer_id -> NULL_VIOLATION) row.
_MIXED_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
    ",Bob Jones,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
)

_HEADER_ONLY_CSV = "customer_id,name,country,birth_date,event_ts,signup_country\n"


def _load_customers_config() -> DatasetConfig:
    return load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)


def _collect_chunks(
    csv_path: Path, config: DatasetConfig
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Flatten every ``process_chunks()`` chunk into one ``(valid, invalid)``
    pair -- these small test fixtures always fit in a single chunk, but this
    stays correct even if that changes."""
    all_valid: list[dict[str, object]] = []
    all_invalid: list[dict[str, object]] = []
    for valid_rows, invalid_rows in process_chunks(csv_path, config):
        all_valid.extend(valid_rows)
        all_invalid.extend(invalid_rows)
    return all_valid, all_invalid


def test_valid_rows_bulk_inserted(tmp_path: Path, oracle_cursor: oracledb.Cursor) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_ALL_VALID_CSV, encoding="utf-8")
    config = _load_customers_config()

    valid_rows, invalid_rows = _collect_chunks(csv_path, config)
    assert len(valid_rows) == 3
    assert invalid_rows == []

    load.insert_rows(
        oracle_cursor,
        table=config.oracle.valid_table,
        columns=[c.name for c in config.columns],
        rows=valid_rows,
    )
    oracle_cursor.connection.commit()

    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (count,) = oracle_cursor.fetchone()
    assert count == 3


def test_invalid_rows_bulk_inserted(tmp_path: Path, oracle_cursor: oracledb.Cursor) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_MIXED_CSV, encoding="utf-8")
    config = _load_customers_config()

    valid_rows, invalid_rows = _collect_chunks(csv_path, config)
    assert len(valid_rows) == 1
    assert len(invalid_rows) == 1

    invalid_columns = [c.name for c in config.columns] + list(load.INVALID_ROW_SUFFIX_COLUMNS)
    load.insert_rows(
        oracle_cursor,
        table=config.oracle.invalid_table,
        columns=invalid_columns,
        rows=invalid_rows,
    )
    oracle_cursor.connection.commit()

    oracle_cursor.execute(
        "SELECT error_code, error_message, source_file, row_number, raw_line "
        "FROM customers_invalid"
    )
    rows = oracle_cursor.fetchall()
    assert len(rows) == 1
    error_code, error_message, source_file, row_number, raw_line = rows[0]
    assert error_code == "NULL_VIOLATION"
    assert error_message
    assert source_file == csv_path.name
    assert row_number == 2
    assert raw_line is not None


def test_ingestion_metadata_recorded(tmp_path: Path, oracle_cursor: oracledb.Cursor) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_MIXED_CSV, encoding="utf-8")
    config = _load_customers_config()

    valid_rows, invalid_rows = _collect_chunks(csv_path, config)
    invalid_columns = [c.name for c in config.columns] + list(load.INVALID_ROW_SUFFIX_COLUMNS)

    load.insert_rows(
        oracle_cursor,
        table=config.oracle.valid_table,
        columns=[c.name for c in config.columns],
        rows=valid_rows,
    )
    load.insert_rows(
        oracle_cursor,
        table=config.oracle.invalid_table,
        columns=invalid_columns,
        rows=invalid_rows,
    )

    checksum = load.sha256_file(csv_path)
    load.record_ingestion(
        oracle_cursor,
        dataset="customers",
        file_name=csv_path.name,
        checksum=checksum,
        total_rows=len(valid_rows) + len(invalid_rows),
        valid_rows=len(valid_rows),
        invalid_rows=len(invalid_rows),
        status="SUCCESS_WITH_INVALID_ROWS",
    )
    oracle_cursor.connection.commit()

    oracle_cursor.execute(
        "SELECT dataset, file_name, checksum, total_rows, valid_rows, invalid_rows, status "
        "FROM ingestion_metadata WHERE dataset = :dataset AND checksum = :checksum",
        {"dataset": "customers", "checksum": checksum},
    )
    rows = oracle_cursor.fetchall()
    assert len(rows) == 1
    dataset, file_name, db_checksum, total_rows, db_valid_rows, db_invalid_rows, status = rows[0]
    assert dataset == "customers"
    assert file_name == csv_path.name
    assert db_checksum == checksum
    assert total_rows == 2
    assert db_valid_rows == 1
    assert db_invalid_rows == 1
    assert status == "SUCCESS_WITH_INVALID_ROWS"


def test_reprocess_is_idempotent(tmp_path: Path, oracle_cursor: oracledb.Cursor) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_ALL_VALID_CSV, encoding="utf-8")
    config = _load_customers_config()
    valid_rows, invalid_rows = _collect_chunks(csv_path, config)
    assert invalid_rows == []

    load.insert_rows(
        oracle_cursor,
        table=config.oracle.valid_table,
        columns=[c.name for c in config.columns],
        rows=valid_rows,
    )
    checksum = load.sha256_file(csv_path)
    load.record_ingestion(
        oracle_cursor,
        dataset="customers",
        file_name=csv_path.name,
        checksum=checksum,
        total_rows=len(valid_rows),
        valid_rows=len(valid_rows),
        invalid_rows=0,
        status="SUCCESS",
    )
    oracle_cursor.connection.commit()

    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (count_before,) = oracle_cursor.fetchone()

    result = load.find_existing_ingestion(oracle_cursor, dataset="customers", checksum=checksum)
    assert result == {
        "total_rows": len(valid_rows),
        "valid_rows": len(valid_rows),
        "invalid_rows": 0,
        "status": "SUCCESS",
    }

    # find_existing_ingestion is query-only -- never re-inserts anything itself.
    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (count_after,) = oracle_cursor.fetchone()
    assert count_after == count_before


def test_duplicate_checksum_raises_integrity_error(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_ALL_VALID_CSV, encoding="utf-8")
    checksum = load.sha256_file(csv_path)

    load.record_ingestion(
        oracle_cursor,
        dataset="customers",
        file_name="customers_a.csv",
        checksum=checksum,
        total_rows=3,
        valid_rows=3,
        invalid_rows=0,
        status="SUCCESS",
    )
    oracle_cursor.connection.commit()

    with pytest.raises(oracledb.IntegrityError) as exc_info:
        load.record_ingestion(
            oracle_cursor,
            dataset="customers",
            file_name="customers_b.csv",
            checksum=checksum,
            total_rows=3,
            valid_rows=3,
            invalid_rows=0,
            status="SUCCESS",
        )
    (error_obj,) = exc_info.value.args
    assert error_obj.full_code == "ORA-00001"
    oracle_cursor.connection.rollback()


def test_renamed_file_same_checksum_is_treated_as_same_file(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_a.csv"
    csv_path.write_text(_ALL_VALID_CSV, encoding="utf-8")
    checksum = load.sha256_file(csv_path)

    load.record_ingestion(
        oracle_cursor,
        dataset="customers",
        file_name="customers_a.csv",
        checksum=checksum,
        total_rows=3,
        valid_rows=3,
        invalid_rows=0,
        status="SUCCESS",
    )
    oracle_cursor.connection.commit()

    # Looked up by (dataset, checksum) only -- "customers_b.csv" never enters this call.
    result = load.find_existing_ingestion(oracle_cursor, dataset="customers", checksum=checksum)
    assert result is not None
    assert result["total_rows"] == 3
    assert result["status"] == "SUCCESS"


def test_insert_rows_skips_executemany_for_empty_list(oracle_cursor: oracledb.Cursor) -> None:
    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (count_before,) = oracle_cursor.fetchone()

    load.insert_rows(oracle_cursor, table="customers_valid", columns=["customer_id"], rows=[])

    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    (count_after,) = oracle_cursor.fetchone()
    assert count_after == count_before


def test_oversized_value_raises_database_error(oracle_cursor: oracledb.Cursor) -> None:
    config = _load_customers_config()
    oversized_row = {
        "customer_id": "x" * 65,  # customers_invalid.customer_id is VARCHAR2(64)
        "name": "Oversized",
        "country": "US",
        "birth_date": "1990-01-01",
        "event_ts": "2026-01-01T00:00:00+0000",
        "signup_country": "US",
        "error_code": "TYPE_MISMATCH",
        "error_message": "oversized value",
        "source_file": "customers_oversized.csv",
        "row_number": 1,
        "raw_line": "x" * 65,
    }
    invalid_columns = [c.name for c in config.columns] + list(load.INVALID_ROW_SUFFIX_COLUMNS)

    with pytest.raises(oracledb.DatabaseError):
        load.insert_rows(
            oracle_cursor,
            table=config.oracle.invalid_table,
            columns=invalid_columns,
            rows=[oversized_row],
        )
    oracle_cursor.connection.rollback()


def test_zero_row_file_still_records_metadata_row(
    tmp_path: Path, oracle_cursor: oracledb.Cursor
) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_HEADER_ONLY_CSV, encoding="utf-8")
    config = _load_customers_config()

    chunks = list(process_chunks(csv_path, config))
    assert chunks == []  # D-20: a valid header with zero data rows yields no chunks at all

    checksum = load.sha256_file(csv_path)
    load.record_ingestion(
        oracle_cursor,
        dataset="customers",
        file_name=csv_path.name,
        checksum=checksum,
        total_rows=0,
        valid_rows=0,
        invalid_rows=0,
        status="SUCCESS",
    )
    oracle_cursor.connection.commit()

    oracle_cursor.execute(
        "SELECT total_rows, valid_rows, invalid_rows, status FROM ingestion_metadata "
        "WHERE dataset = :dataset AND checksum = :checksum",
        {"dataset": "customers", "checksum": checksum},
    )
    rows = oracle_cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (0, 0, 0, "SUCCESS")
