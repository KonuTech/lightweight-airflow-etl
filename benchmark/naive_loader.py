"""Naive, deliberately throwaway per-row Oracle INSERT loop (TEST-04, D-01) --
the naive baseline the benchmark compares against
``csv_processor.load.insert_rows()``'s real array-bind bulk-insert path.

Lives outside ``packages/csv-processor`` (D-04) -- never confused with, and
never imported by, the reusable engine. This module is intentionally the ONE
place in the whole repo that issues one ``cursor.execute()`` per row in a
loop; the array-bind bulk-insert call is never used anywhere in this file
(06-RESEARCH.md/PITFALLS.md's "Performance Trap 1" -- D-01's exact
requirement is a genuine per-row round trip, NOT the bulk call run at
``chunk_size=1``, which would still batch-bind internally and fail to
reproduce the real per-round-trip cost this benchmark exists to measure).
"""

from __future__ import annotations

import oracledb

from csv_processor.config.models import DatasetConfig, is_safe_identifier


def run_naive(
    cursor: oracledb.Cursor,
    config: DatasetConfig,
    chunk_valid: list[dict[str, object]],
) -> None:
    """Insert ``chunk_valid`` into ``config.oracle.valid_table`` one row at a
    time -- genuinely one ``cursor.execute()`` call per row.

    Mirrors ``csv_processor.load.insert_rows()``'s own defense-in-depth
    ``is_safe_identifier()`` re-check (T-06-04, mirroring T-04-01) on
    ``table``/every column name immediately before interpolating them into
    the INSERT string -- Oracle has no bind-parameter mechanism for
    identifiers, only values, so this check is the only defense available.

    Args:
        cursor: An open Oracle cursor (caller owns the connection/commit).
        config: The dataset's validated config -- supplies the target valid
            table and column list.
        chunk_valid: One chunk's valid-row dicts, produced by
            ``csv_processor.engine.process_chunks()`` -- the exact same
            generator the bulk benchmark path also consumes (D-03), so only
            the write strategy below differs between the two benchmark runs.

    Raises:
        ValueError: The table name or any column name is not a safe SQL
            identifier.
    """
    if not chunk_valid:
        return

    table = config.oracle.valid_table
    columns = [column.name for column in config.columns]

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

    # D-01: genuinely one execute() call per row -- never a single
    # array-bind call for the whole chunk, at any chunk size, anywhere in
    # this file.
    for row in chunk_valid:
        cursor.execute(sql, row)
