"""Oracle bulk-load primitives (LOAD-01 through LOAD-04) -- the ONLY write
path into ``<DATASET>_VALID``/``<DATASET>_INVALID``/``ingestion_metadata``.

One connection per ``csv_processor.engine.process()`` call is the intended
usage (Plan 04-02, ARCHITECTURE.md "one connection per process() call"):
``get_connection()`` opens it, the caller runs the idempotency check
(``find_existing_ingestion``), streams chunks through ``insert_rows``, then
calls ``record_ingestion`` once, and finally commits -- this module never
calls ``connection.commit()``/``rollback()`` itself, leaving transaction
boundaries entirely to the caller (D-02's all-or-nothing atomicity).

Connection credentials are read from this project's REAL docker-compose.yml
env var names (``ORACLE_APP_USER``/``ORACLE_APP_USER_PASSWORD``), not
``scripts/verify_environment.py``'s hardcoded ``admin``/``admin`` literals
(04-RESEARCH.md Pitfall 6) -- ``ORACLE_PASSWORD`` is a DIFFERENT env var (the
container's SYS password), never the app connection credential.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import oracledb

from csv_processor.config.models import is_safe_identifier

_READ_CHUNK = 1 << 20

# The fixed suffix columns every <DATASET>_INVALID table carries beyond its
# original data columns (Phase 1 DDL, 02_customers.sql/03_orders.sql +
# 04_widen_invalid_columns.sql) -- Phase 3's engine.py invalid-row dicts
# already carry exactly these keys (D-09), verbatim, no adapter needed.
INVALID_ROW_SUFFIX_COLUMNS = (
    "error_code",
    "error_message",
    "source_file",
    "row_number",
    "raw_line",
)


def sha256_file(path: Path) -> str:
    """Hash a file without reading it into memory.

    Reimplements ``tools/corpus/digests.py``'s exact chunked-read shape
    locally -- never imported across the ``tools/`` package boundary, since
    ``tools/`` is a dev/test-only namespace package not installed into the
    Airflow worker container (04-RESEARCH.md Pitfall 5).

    Args:
        path: File to hash.

    Returns:
        The lowercase hex SHA-256 digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def oracle_dsn() -> str:
    """The Oracle connect-string, env-var-first (INFRA-03)."""
    return os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")


def oracle_user() -> str:
    """The Oracle app-schema username -- ``ORACLE_APP_USER``, this project's
    real docker-compose.yml env var name, never ``ORACLE_USER`` (invented) or
    ``ORACLE_PASSWORD`` (a different variable -- the container's SYS
    password)."""
    return os.environ.get("ORACLE_APP_USER", "admin")


def oracle_password() -> str:
    """The Oracle app-schema password -- ``ORACLE_APP_USER_PASSWORD``, this
    project's real docker-compose.yml env var name."""
    return os.environ.get("ORACLE_APP_USER_PASSWORD", "admin")


def get_connection() -> oracledb.Connection:
    """Open one real Oracle connection using this project's env-var-first
    credentials (falls back to the single documented dev credential pair,
    ``admin``/``admin``, per INFRA-03)."""
    return oracledb.connect(user=oracle_user(), password=oracle_password(), dsn=oracle_dsn())


def insert_rows(
    cursor: oracledb.Cursor,
    *,
    table: str,
    columns: list[str],
    rows: list[dict[str, object]],
) -> None:
    """Bulk-insert ``rows`` into ``table`` via ``cursor.executemany()``
    (LOAD-01/LOAD-02) -- never a per-row ``INSERT`` loop.

    Returns immediately, without calling ``cursor.executemany()``, when
    ``rows`` is empty -- an all-invalid chunk (empty ``valid_rows``) or an
    all-valid chunk (empty ``invalid_rows``) never triggers an empty-array
    ``executemany()`` call.

    ``table`` and every entry in ``columns`` are re-validated via
    ``is_safe_identifier()`` immediately before building the INSERT string --
    defense-in-depth (T-04-01) against a future caller that bypasses
    ``OracleTargetSpec``/``ColumnSpec``'s own config-load-time validation.
    Oracle has no bind-parameter mechanism for identifiers, only values, so
    this check is the only defense available before the identifiers are
    interpolated into the SQL text.

    Args:
        cursor: An open Oracle cursor (caller owns the connection/commit).
        table: The target table name (e.g. ``"customers_valid"``).
        columns: Column names, in bind order -- becomes both the INSERT's
            column list and its named-bind list (``:col_name``).
        rows: One dict per row, keys equal to ``columns`` -- Phase 3's
            ``process_chunks()`` valid/invalid row dicts already carry this
            exact shape, no adapter needed.

    Raises:
        ValueError: ``table`` or any entry in ``columns`` is not a safe SQL
            identifier.
        oracledb.DatabaseError: A row-level Oracle error (e.g. a value too
            large for its declared column width, ``ORA-01461``) -- propagates
            uncaught; the caller (Plan 04-02's ``process()``) translates it to
            ``DATABASE_ERROR``.
    """
    if not rows:
        return

    if not is_safe_identifier(table):
        msg = f"unsafe SQL identifier for table name: {table!r}"
        raise ValueError(msg)
    for column in columns:
        if not is_safe_identifier(column):
            msg = f"unsafe SQL identifier for column name: {column!r}"
            raise ValueError(msg)

    column_list = ", ".join(columns)
    bind_list = ", ".join(f":{column}" for column in columns)
    sql = f"INSERT INTO {table} ({column_list}) VALUES ({bind_list})"
    cursor.executemany(sql, rows)


def find_existing_ingestion(
    cursor: oracledb.Cursor, *, dataset: str, checksum: str
) -> dict[str, object] | None:
    """Look up a previously-recorded ingestion by ``(dataset, checksum)``
    ONLY -- never ``file_name`` (LOAD-04, D-01).

    Two differently-named, byte-identical files for the same dataset are
    treated as the SAME processed file: this is the app-level fast-path
    idempotency check, sitting in addition to (not instead of) the DB's own
    ``UNIQUE(dataset, checksum)`` constraint (D-05).

    Args:
        cursor: An open Oracle cursor.
        dataset: The dataset name (e.g. ``"customers"``).
        checksum: The file's SHA-256 hex digest.

    Returns:
        ``None`` on no match, else ``{"total_rows", "valid_rows",
        "invalid_rows", "status"}`` -- the previously-recorded outcome, read
        back verbatim (never re-derived).
    """
    cursor.execute(
        "SELECT total_rows, valid_rows, invalid_rows, status "
        "FROM ingestion_metadata WHERE dataset = :dataset AND checksum = :checksum",
        {"dataset": dataset, "checksum": checksum},
    )
    row = cursor.fetchone()
    if row is None:
        return None
    total_rows, valid_rows, invalid_rows, status = row
    return {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "status": status,
    }


def record_ingestion(
    cursor: oracledb.Cursor,
    *,
    dataset: str,
    file_name: str,
    checksum: str,
    total_rows: int,
    valid_rows: int,
    invalid_rows: int,
    status: str,
) -> None:
    """Record one processed file in ``ingestion_metadata`` (LOAD-03) -- a
    plain, insert-only ``INSERT`` (T-04-02: this module never executes an
    ``UPDATE`` against ``ingestion_metadata``).

    Never includes ``id``/``processed_at`` -- both have Oracle-side defaults
    (``GENERATED ALWAYS AS IDENTITY`` / ``DEFAULT SYSTIMESTAMP``).

    A conflicting re-record for the same ``(dataset, checksum)`` surfaces via
    ``oracledb.IntegrityError`` (``full_code == "ORA-00001"``, the DB's
    ``UNIQUE(dataset, checksum)`` constraint, D-05) -- this function lets it
    propagate uncaught; the caller decides how to handle the race (Plan
    04-02's ``process()`` re-reads the winner's row via
    ``find_existing_ingestion`` and returns its result, D-01).

    Args:
        cursor: An open Oracle cursor (caller owns the connection/commit).
        dataset: The dataset name.
        file_name: The original file's basename (lineage/reporting only --
            never part of the idempotency lookup key, see
            ``find_existing_ingestion``).
        checksum: The file's SHA-256 hex digest.
        total_rows: Total rows processed.
        valid_rows: Rows that passed validation.
        invalid_rows: Rows that failed validation.
        status: The ``Status`` enum's ``.value`` string.
    """
    cursor.execute(
        "INSERT INTO ingestion_metadata "
        "(dataset, file_name, checksum, total_rows, valid_rows, invalid_rows, status) "
        "VALUES (:dataset, :file_name, :checksum, :total_rows, :valid_rows, "
        ":invalid_rows, :status)",
        {
            "dataset": dataset,
            "file_name": file_name,
            "checksum": checksum,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "status": status,
        },
    )
