"""``process_chunks()`` -- Phase 3's public generator surface (D-11) -- and
``process()``, the phase's public entrypoint (ENGINE-08).

``process(file_path, config) -> ProcessingResult`` owns the whole
detect->parse->validate->normalize->chunk->load sequence and all
status/exception translation, assembling Plan 04-01's ``load.py`` primitives
around ``process_chunks()`` (D-07's exact generator contract). See
04-RESEARCH.md's "System Architecture Diagram" for the exact step sequence
this function implements.

No reference-repo file analog exists for this module (03-PATTERNS.md); it
follows ``csv_processor.config.loader``'s docstring/error-wrapping
convention and 03-RESEARCH.md's own verified ``itertools.batched`` chunking
pattern (Pattern 5).
"""

from __future__ import annotations

import itertools
import time
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import oracledb
from pydantic import ValidationError

from csv_processor import errors, load, source, validate
from csv_processor.config.models import DatasetConfig
from csv_processor.errors import StructuralValidationError
from csv_processor.models import ProcessingResult, Status


def process_chunks(
    file_path: Path, config: DatasetConfig
) -> Iterator[tuple[list[dict[str, object]], list[dict[str, object]]]]:
    """Detect once, then stream ``config.processing.chunk_size``-row chunks.

    Per row: a structural field-count mismatch (D-13 short-circuit) always
    wins -- ``validate.check_row`` is never called on that row, and its
    ``error_code`` is always ``WRONG_COLUMN_COUNT``. Once a row is
    structurally sound, ``validate.check_row`` runs the full
    nullability-then-type check (D-14/D-15) across every column.

    Args:
        file_path: The CSV file to process.
        config: The dataset's validated config.

    Yields:
        One ``(valid_rows, invalid_rows)`` tuple per chunk (D-11) -- only
        one chunk's rows are ever in memory at once (ENGINE-07's
        bounded-memory guarantee). A chunk that ends up with zero rows of
        one kind still yields, e.g. an all-valid chunk yields
        ``(valid_rows, [])``. A file with a valid header but zero data rows
        yields no chunks at all (D-20 -- not an error case).

    Raises:
        StructuralValidationError: A whole-file structural problem (D-16..
            D-20) was found before any row was processed -- the ONLY
            exception this generator ever lets propagate; every row-level
            problem becomes an invalid-row entry instead (T-03-08's
            mitigation -- no crafted row can crash this generator
            mid-file).
    """
    source_file = file_path.name  # D-08 -- basename only
    text_stream, paired_rows, header = source.prepare_source(file_path, config)
    try:
        row_number = 0
        for batch in itertools.batched(paired_rows, config.processing.chunk_size):
            valid_rows: list[dict[str, object]] = []
            invalid_rows: list[dict[str, object]] = []

            for raw_row, raw_line in batch:
                row_number += 1

                if len(raw_row) != len(header):
                    # D-05: None for a structurally absent trailing field,
                    # never "" -- distinguishes "absent" from
                    # "present-but-empty". A DIFFERENT variable name than the
                    # main-path `row_dict` below (mypy note): its `object`
                    # value type (to allow `None`) must never widen the main
                    # path's `dict[str, str]` inference, which
                    # `validate.check_row()`/`normalize_row()` require.
                    partial_row: dict[str, object] = {
                        name: (raw_row[i] if i < len(raw_row) else None)
                        for i, name in enumerate(header)
                    }
                    invalid_rows.append(
                        {
                            **partial_row,
                            "error_code": errors.WRONG_COLUMN_COUNT,
                            "error_message": (f"expected {len(header)} fields, got {len(raw_row)}"),
                            "source_file": source_file,
                            "row_number": row_number,
                            "raw_line": raw_line,
                        }
                    )
                    continue  # D-13 short-circuit -- check_row never called

                # strict=True is safe here: the `len(raw_row) != len(header)`
                # branch above already `continue`s on any length mismatch, so
                # this zip() is only ever reached with equal-length inputs.
                row_dict = dict(zip(header, raw_row, strict=True))
                # CR-01's per-row companion fix: a `required: false` column
                # legitimately absent from the detected header (source.py's
                # missing-column check no longer flags it) would otherwise
                # KeyError the moment validate.check_row()/normalize_row()'s
                # row[column.name] lookups run. Backfilling it as an empty
                # string treats "never in the file" identically to
                # "present but blank" -- the column's own `nullable` flag
                # then governs it exactly as it already does for any other
                # blank value.
                for column in config.columns:
                    if column.name not in row_dict:
                        row_dict[column.name] = ""
                error_code, error_message, _error_column = validate.check_row(row_dict, config)
                if error_code:
                    invalid_rows.append(
                        {
                            **row_dict,
                            "error_code": error_code,
                            "error_message": error_message,
                            "source_file": source_file,
                            "row_number": row_number,
                            "raw_line": raw_line,
                        }
                    )
                else:
                    valid_rows.append(validate.normalize_row(row_dict, config))

            yield valid_rows, invalid_rows
    finally:
        text_stream.close()


def _build_result(
    status: Status,
    config: DatasetConfig,
    file_path: Path,
    start: float,
    *,
    checksum: str | None,
    total_rows: int = 0,
    valid_rows: int = 0,
    invalid_rows: int = 0,
) -> ProcessingResult:
    """Assemble a ``ProcessingResult`` with real elapsed ``duration_seconds``
    (``time.monotonic() - start``) -- the one construction site every
    ``process()`` return path shares, so every field's zero/``None``
    defaults stay consistent across status paths.
    """
    return ProcessingResult(
        status=status,
        dataset=config.dataset,
        file_name=file_path.name,
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        duration_seconds=time.monotonic() - start,
        checksum=checksum,
    )


def _result_from_existing(
    existing: dict[str, object],
    config: DatasetConfig,
    file_path: Path,
    start: float,
    *,
    checksum: str,
) -> ProcessingResult:
    """Build a ``ProcessingResult`` from a ``load.find_existing_ingestion()``
    row (D-01's idempotency short-circuit -- read back verbatim, never
    re-derived). Oracle's driver returns untyped ``object`` values per column
    (mypy note); the `int`/`str` casts below reflect the ``ingestion_metadata``
    schema's own known column types (Phase 1 DDL), not a runtime guess.
    """
    return _build_result(
        Status(cast(str, existing["status"])),
        config,
        file_path,
        start,
        checksum=checksum,
        total_rows=cast(int, existing["total_rows"]),
        valid_rows=cast(int, existing["valid_rows"]),
        invalid_rows=cast(int, existing["invalid_rows"]),
    )


def process(file_path: Path, config: DatasetConfig) -> ProcessingResult:
    """The phase's public entrypoint (ENGINE-08) -- owns the whole
    detect->parse->validate->normalize->chunk->load sequence and all
    status/exception translation.

    Sequence (04-RESEARCH.md's "System Architecture Diagram"):

    1. Re-validate ``config`` (cheap, already a Pydantic model) -- a failure
       here means Phase 5's DAG passed a config that no longer validates
       (e.g. stale runtime conf) -> ``CONFIGURATION_ERROR``, before any file
       I/O or Oracle connection.
    2. Confirm ``file_path`` exists -> ``FILE_NOT_FOUND`` if not, still
       before any Oracle connection.
    3. Compute the SHA-256 checksum FIRST (pure file I/O, no Oracle
       dependency) -- this is why ``checksum`` is populated on every
       subsequent failure path.
    4. Open exactly ONE Oracle connection for the whole call (D-02).
    5. Check ``ingestion_metadata`` for an existing ``(dataset, checksum)``
       record (D-01) -- found means return the ORIGINAL recorded outcome,
       never re-derived, without calling ``process_chunks()`` at all.
    6. Not found: stream ``process_chunks()``'s chunks into
       ``load.insert_rows`` for both the valid and invalid tables (D-03: one
       ``executemany()`` call per chunk, array size == ``chunk_size``).
    7. Record the file in ``ingestion_metadata`` and commit -- ALL of step 6
       + this insert commit together in one transaction (D-02). A
       concurrent winner's ``oracledb.IntegrityError``/``ORA-00001`` here is
       not an error: roll back this call's own uncommitted inserts and
       return the winner's already-committed result instead.

    Args:
        file_path: The CSV file to process.
        config: The dataset's validated config.

    Returns:
        A ``ProcessingResult`` with exactly one of the 7 closed ``Status``
        values -- never raises; every exception this function's own
        sequence can produce is caught and translated into a status
        instead.
    """
    start = time.monotonic()

    try:
        DatasetConfig.model_validate(config.model_dump(mode="json"))
    except ValidationError:
        return _build_result(Status.CONFIGURATION_ERROR, config, file_path, start, checksum=None)

    if not file_path.is_file():
        return _build_result(Status.FILE_NOT_FOUND, config, file_path, start, checksum=None)

    checksum = load.sha256_file(file_path)
    total_rows = valid_count = invalid_count = 0
    connection: oracledb.Connection | None = None

    try:
        connection = load.get_connection()
        cursor = connection.cursor()

        existing = load.find_existing_ingestion(cursor, dataset=config.dataset, checksum=checksum)
        if existing is not None:
            return _result_from_existing(existing, config, file_path, start, checksum=checksum)

        valid_columns = [c.name for c in config.columns]
        invalid_columns = valid_columns + list(load.INVALID_ROW_SUFFIX_COLUMNS)

        for chunk_valid, chunk_invalid in process_chunks(file_path, config):
            load.insert_rows(
                cursor, table=config.oracle.valid_table, columns=valid_columns, rows=chunk_valid
            )
            load.insert_rows(
                cursor,
                table=config.oracle.invalid_table,
                columns=invalid_columns,
                rows=chunk_invalid,
            )
            total_rows += len(chunk_valid) + len(chunk_invalid)
            valid_count += len(chunk_valid)
            invalid_count += len(chunk_invalid)

        status = Status.SUCCESS if invalid_count == 0 else Status.SUCCESS_WITH_INVALID_ROWS

        try:
            load.record_ingestion(
                cursor,
                dataset=config.dataset,
                file_name=file_path.name,
                checksum=checksum,
                total_rows=total_rows,
                valid_rows=valid_count,
                invalid_rows=invalid_count,
                status=status.value,
            )
        except oracledb.IntegrityError as exc:
            (error_obj,) = exc.args
            if getattr(error_obj, "full_code", None) != "ORA-00001":
                raise
            # D-01: a concurrent process() call for the same (dataset,
            # checksum) already won -- undo this call's own uncommitted
            # inserts and return the winner's already-committed result.
            connection.rollback()
            existing = load.find_existing_ingestion(
                cursor, dataset=config.dataset, checksum=checksum
            )
            if existing is None:
                msg = "expected a recorded ingestion row after ORA-00001, found none"
                raise RuntimeError(msg) from exc
            return _result_from_existing(existing, config, file_path, start, checksum=checksum)

        connection.commit()
        return _build_result(
            status,
            config,
            file_path,
            start,
            checksum=checksum,
            total_rows=total_rows,
            valid_rows=valid_count,
            invalid_rows=invalid_count,
        )
    except StructuralValidationError:
        if connection is not None:
            connection.rollback()
        return _build_result(Status.INVALID_FILE, config, file_path, start, checksum=checksum)
    except oracledb.Error:
        if connection is not None:
            connection.rollback()
        return _build_result(
            Status.DATABASE_ERROR,
            config,
            file_path,
            start,
            checksum=checksum,
            total_rows=total_rows,
            valid_rows=valid_count,
            invalid_rows=invalid_count,
        )
    except Exception:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:  # noqa: BLE001 -- best-effort rollback, never masks the real error
                pass
        return _build_result(
            Status.PROCESSING_ERROR,
            config,
            file_path,
            start,
            checksum=checksum,
            total_rows=total_rows,
            valid_rows=valid_count,
            invalid_rows=invalid_count,
        )
    finally:
        if connection is not None:
            connection.close()
