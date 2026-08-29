"""``process_chunks()`` -- Phase 3's public generator surface (D-11).

The full ``process()``/``ProcessingResult`` wrapper (status codes, Oracle
loading) is explicitly Phase 4's job (ENGINE-08) -- this module only builds
the chunked valid/invalid row split that Phase 4's future loop will consume
directly for ``executemany()`` binding.

No reference-repo file analog exists for this module (03-PATTERNS.md); it
follows ``csv_processor.config.loader``'s docstring/error-wrapping
convention and 03-RESEARCH.md's own verified ``itertools.batched`` chunking
pattern (Pattern 5).
"""

from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Iterator

from csv_processor import errors, source, validate

if TYPE_CHECKING:
    from pathlib import Path

    from csv_processor.config.models import DatasetConfig


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
                    # "present-but-empty".
                    row_dict: dict[str, object] = {
                        name: (raw_row[i] if i < len(raw_row) else None)
                        for i, name in enumerate(header)
                    }
                    invalid_rows.append(
                        {
                            **row_dict,
                            "error_code": errors.WRONG_COLUMN_COUNT,
                            "error_message": (
                                f"expected {len(header)} fields, got {len(raw_row)}"
                            ),
                            "source_file": source_file,
                            "row_number": row_number,
                            "raw_line": raw_line,
                        }
                    )
                    continue  # D-13 short-circuit -- check_row never called

                row_dict = dict(zip(header, raw_row))
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
