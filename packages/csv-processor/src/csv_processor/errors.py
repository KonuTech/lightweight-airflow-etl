"""Local exception hierarchy for ``csv_processor``'s detection/parsing/validation
pipeline -- never imported from the reference platform's package (CLAUDE.md's
two-tier reuse rule).

Every class here mirrors ``csv_processor.config.errors.ConfigurationError``'s exact
shape (plain ``Exception`` subclass, keyword-only ``context: dict`` constructor) so
callers only ever need to catch one of these named types, never a bare ``Exception``.

Five of the six classes below exist solely to satisfy the Tier-A vendored
``detect/*`` modules' own former reference-package error import -- each vendored
file gets its single import swapped to point at the identically-named class
defined here (one-line substitution, zero behavior change):

- ``CsvDialectDetectionError`` <- ``detect/dialect.py``
- ``EncodingDetectionError``    <- ``detect/encoding.py``
- ``FileInspectionError``       <- ``detect/header.py`` (also reused by Phase 3's
  own ``compression.py`` for decompression-bomb/corrupted-archive errors, matching
  the reference repo's own reuse of this exact class across both concerns)
- ``FilenameParsingError``      <- ``detect/filename.py`` (vendored for parity per
  D-27, no caller in this phase)

``schema.py`` needs no entry here -- grep-confirmed it has zero such import
(unlike the other four Tier-A modules).

``StructuralValidationError`` is this project's own whole-file-reject exception
(D-16, D-23) -- reused for BOTH header-level structural failures (missing/extra/
duplicate column, no header row) AND detect-vs-config mismatches. The two failure
sources stay distinguishable via the ``error_code`` value passed in
``context["error_code"]``, never via a separate exception class (D-16).
"""

from __future__ import annotations


class CsvProcessorError(Exception):
    """Base class for every exception this package raises.

    Never raised directly -- always one of the subclasses below. Provides one
    common type callers can catch if they want "anything went wrong in
    csv_processor" without needing to enumerate every specific subclass.

    Attributes:
        context: Structured details about the failure -- shape varies by
            subclass, but is always a ``dict`` (never ``None``).
    """

    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context


class CsvDialectDetectionError(CsvProcessorError):
    """Raised when ``detect/dialect.py``'s clevercsv-based sniffing fails outright
    (e.g. a degenerate/empty sample) rather than merely returning a low-confidence
    result -- replaces ``detect/dialect.py``'s former reference-package error import.
    """


class EncodingDetectionError(CsvProcessorError):
    """Raised when ``detect/encoding.py``'s BOM-check + charset-normalizer/chardet
    corroboration cannot determine a usable encoding, or when strict decoding
    against the resolved encoding fails -- replaces ``detect/encoding.py``'s
    former reference-package error import.
    """


class FileInspectionError(CsvProcessorError):
    """Raised when ``detect/header.py``'s multi-gate scoring cannot locate a header
    row, and reused by this phase's own ``compression.py`` for
    decompression-bomb/corrupted-archive errors (matching the reference repo's own
    reuse of this exact class across both concerns) -- replaces ``detect/header.py``'s
    former reference-package error import.
    """


class FilenameParsingError(CsvProcessorError):
    """Raised when ``detect/filename.py``'s mask-based filename parsing fails --
    vendored for parity per D-27; this phase has no caller for ``filename.py`` yet.
    Replaces ``detect/filename.py``'s former reference-package error import.
    """


class StructuralValidationError(CsvProcessorError):
    """Whole-file reject -- zero rows processed (D-17).

    Reused for both header-level structural failures (missing/extra/duplicate
    declared column, no header row, empty file) AND detect-vs-config mismatches
    (dialect/encoding/header disagreement, D-16) -- the two failure sources are
    distinguished only via the specific ``error_code`` constant passed in
    ``context["error_code"]``, never via a separate exception class.
    """


# ---------------------------------------------------------------------------
# error_code vocabulary (D-12/D-16) -- the phase's complete set of string
# constants. Defined once here so validate.py/source.py/engine.py in later
# plans import the same constants rather than re-typing string literals.
# ---------------------------------------------------------------------------

# Header-level structural failures (whole-file reject, D-17/D-18/D-19/D-20).
MISSING_REQUIRED_COLUMN = "MISSING_REQUIRED_COLUMN"
EXTRA_UNEXPECTED_COLUMN = "EXTRA_UNEXPECTED_COLUMN"
DUPLICATE_COLUMN_NAME = "DUPLICATE_COLUMN_NAME"
NO_HEADER_ROW = "NO_HEADER_ROW"  # also covers the D-20 zero-byte-file case

# Detect-vs-config mismatches (whole-file reject via the same
# StructuralValidationError, but a distinct DETECT_*-prefixed family, D-16).
DETECT_ENCODING_MISMATCH = "DETECT_ENCODING_MISMATCH"
DETECT_DIALECT_MISMATCH = "DETECT_DIALECT_MISMATCH"

# Row-level failures (per-row invalid entry, rest of file keeps processing).
WRONG_COLUMN_COUNT = "WRONG_COLUMN_COUNT"  # also covers the D-24 blank-line case
NULL_VIOLATION = "NULL_VIOLATION"
TYPE_MISMATCH = "TYPE_MISMATCH"
INVALID_DATE_FORMAT = "INVALID_DATE_FORMAT"
INVALID_TIMESTAMP_FORMAT = "INVALID_TIMESTAMP_FORMAT"
DECIMAL_PRECISION_EXCEEDED = "DECIMAL_PRECISION_EXCEEDED"

# Compressed-input failures (03-04-PLAN.md Task 1, D-29/D-30/D-33) -- both
# raised as FileInspectionError, distinguished by this error_code, matching
# the same "one exception type, distinct error_code" convention as
# StructuralValidationError above.
CORRUPTED_ARCHIVE = "CORRUPTED_ARCHIVE"
DECOMPRESSION_BOMB_EXCEEDED = "DECOMPRESSION_BOMB_EXCEEDED"
