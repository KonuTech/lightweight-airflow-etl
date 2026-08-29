"""Detect-once-per-file source orchestrator (D-25, 03-RESEARCH.md Pattern 1).

``prepare_source()`` runs the whole detect -> cross-check -> header-match
sequence exactly once per file (Anti-Pattern 2: never re-detect per chunk),
then reopens the file fresh for the real streaming read. This is a Tier-B
rewrite -- the reference repo's own ``source.py`` is fully wired into
``dataplat``'s ``Source``/``SchemaRepository``/``RecordChunk`` model and is
not portable as a file (03-PATTERNS.md); only the *sequence* (detect
compression -> decode encoding -> detect dialect -> detect header -> stream
rows) is read from it. Compression detection/opening is 03-04's job -- this
plan's own ``_open_raw_stream`` is a plain, uncompressed file open; every
other function here keeps calling it by name so nothing else changes when
that lands.

Every whole-file structural reject raises the SAME
``errors.StructuralValidationError`` (D-23) -- never an Airflow-aware
exception type (ENGINE-09) and never a family of exception subclasses --
distinguished only by the ``error_code`` value in ``context["error_code"]``
(D-16).
"""

from __future__ import annotations

import codecs
import csv
import io
from typing import TYPE_CHECKING, BinaryIO, Iterator, TextIO

from csv_processor import detect, errors
from csv_processor.errors import StructuralValidationError

if TYPE_CHECKING:
    from pathlib import Path

    from csv_processor.config.models import DatasetConfig

# T-03-06's mitigation: set explicitly at module scope rather than trust
# Python's unstated 131072-byte default (03-RESEARCH.md Pattern 6), so an
# unterminated-quote runaway field fails predictably.
csv.field_size_limit(1_048_576)

# 64 KiB -- matches detect/encoding.py's own documented sample convention
# (03-RESEARCH.md Pattern 1).
SAMPLE_BYTES = 65_536


def _open_raw_stream(file_path: Path) -> BinaryIO:
    """Open ``file_path``'s raw byte stream.

    This task's own body -- no compression awareness yet. 03-04 replaces
    only this function's implementation with magic-byte-sniffed gzip/zip
    opening (D-29/D-30); every other function in this module keeps calling
    this function by name, so nothing else changes when that lands.
    """
    return file_path.open("rb")


class _LineCapturingTextStream:
    """Wraps a text stream, remembering the last line ``__next__`` yielded.

    ``engine.py`` pairs each parsed row with its own raw source line
    (D-06's ``raw_line`` field) -- ``csv.reader`` itself discards the
    original line text once parsed, so this thin wrapper captures it as a
    side effect of the underlying text stream's own iteration, which
    ``csv.reader`` drives internally.

    Known, accepted limitation: a row spanning multiple physical lines (an
    embedded newline inside a quoted field) captures only the LAST physical
    line, not the full span -- acceptable since ``byte_level_hard`` fixtures
    are tested against parsing primitives directly, never this path
    (03-RESEARCH.md Pitfall 3).
    """

    def __init__(self, text_stream: TextIO) -> None:
        self._text_stream = text_stream
        self.last_line: str = ""

    def __iter__(self) -> _LineCapturingTextStream:
        return self

    def __next__(self) -> str:
        line = next(self._text_stream)
        self.last_line = line
        return line


def _rows_with_raw_line(
    reader: Iterator[list[str]], wrapper: _LineCapturingTextStream
) -> Iterator[tuple[list[str], str]]:
    """Pair each parsed row with the raw line ``wrapper`` captured for it.

    Deliberately not ``itertools.batched(reader, ...)`` directly -- pairing
    happens here so ``raw_line`` (D-06) survives into ``engine.py``, which
    still calls ``itertools.batched`` on THIS paired iterator, keeping
    ENGINE-07's "record-count chunking, never byte/line offset chunking"
    contract unchanged.
    """
    for row in reader:
        yield row, wrapper.last_line


def prepare_source(
    file_path: Path, config: DatasetConfig
) -> tuple[TextIO, Iterator[tuple[list[str], str]], tuple[str, ...]]:
    """Detect once, cross-check against config, then open a fresh read stream.

    PASS 1 (detection): reads one bounded sample, detects encoding/dialect/
    header, and cross-checks each against ``config``'s declared values
    (D-25/D-28) -- a high-confidence disagreement is a whole-file reject;
    a low-confidence/undetermined one silently defers to config. PASS 2
    (real read): reopens the file fresh with the resolved codec/dialect and
    returns a lazy row iterator that has already skipped the header line.

    Args:
        file_path: The CSV file to read.
        config: The dataset's validated config.

    Returns:
        ``(text_stream, paired_rows, header)`` -- the open text stream
        (the caller must close it once done, e.g. via ``try``/``finally``),
        a lazy iterator of ``(row, raw_line)`` pairs (never the header row
        itself), and the detected header tuple (D-21 order, not
        ``config.columns``' declared order).

    Raises:
        StructuralValidationError: A whole-file structural problem was
            found -- a detect-vs-config mismatch (``DETECT_ENCODING_MISMATCH``/
            ``DETECT_DIALECT_MISMATCH``, D-16), a duplicate header column
            name (``DUPLICATE_COLUMN_NAME``), no header row at all
            (``NO_HEADER_ROW`` -- also covers a genuinely empty file, D-20),
            a header missing a declared column (``MISSING_REQUIRED_COLUMN``),
            or a header with an undeclared column
            (``EXTRA_UNEXPECTED_COLUMN``).
    """
    raw_stream = _open_raw_stream(file_path)
    try:
        sample = raw_stream.read(SAMPLE_BYTES)
    finally:
        raw_stream.close()

    contract_encoding_name = codecs.lookup(config.csv.encoding).name
    enc_detection = detect.detect_encoding(sample, contract_encoding=None)
    detected_name = codecs.lookup(enc_detection.encoding).name
    if (
        enc_detection.source == "detected"
        and detected_name != contract_encoding_name
        # A sample containing only ASCII bytes decodes identically under
        # any ASCII-superset codec (utf-8, latin-1, cp1252, ...) -- a
        # "detected ascii" result is never a real conflict with a
        # declared "utf-8" (or similar) encoding, only a genuine
        # non-ASCII-vs-declared mismatch (e.g. detected cp1250) is.
        and detected_name != "ascii"
    ):
        raise StructuralValidationError(
            f"detected encoding {enc_detection.encoding!r} disagrees with "
            f"config.json's declared encoding {config.csv.encoding!r}",
            context={
                "error_code": errors.DETECT_ENCODING_MISMATCH,
                "detected": enc_detection.encoding,
                "configured": config.csv.encoding,
            },
        )
    # source == "undetermined" defers to config silently (D-28); "bom" and
    # "contract" never conflict by construction.

    if enc_detection.source in ("bom", "detected"):
        decoded_sample = detect.decode_strict(sample, enc_detection)
        decode_codec = enc_detection.encoding
    else:
        # D-26: always strip a UTF-8 BOM even when detection deferred to
        # config.
        decode_codec = "utf-8-sig" if contract_encoding_name == "utf-8" else config.csv.encoding
        decoded_sample = sample.decode(decode_codec)

    dialect_detection = detect.detect_dialect(decoded_sample, contract_delimiter=None)
    if not dialect_detection.declined and dialect_detection.delimiter != config.csv.delimiter:
        raise StructuralValidationError(
            f"detected delimiter {dialect_detection.delimiter!r} disagrees "
            f"with config.json's declared delimiter {config.csv.delimiter!r}",
            context={
                "error_code": errors.DETECT_DIALECT_MISMATCH,
                "detected": dialect_detection.delimiter,
                "configured": config.csv.delimiter,
            },
        )
    # .declined always defers (D-28).

    sample_rows: list[list[str]] = list(
        csv.reader(
            io.StringIO(decoded_sample),
            delimiter=config.csv.delimiter,
            quotechar=config.csv.quotechar,
            doublequote=config.csv.doublequote,
            escapechar=config.csv.escapechar,
        )
    )

    try:
        header_detection = detect.detect_header(rows=tuple(tuple(r) for r in sample_rows))
    except errors.FileInspectionError as exc:
        raise StructuralValidationError(
            f"header has a duplicate column name: {exc}",
            context={"error_code": errors.DUPLICATE_COLUMN_NAME},
        ) from exc

    if not header_detection.has_header:
        # Also covers the D-20 zero-byte-file case: sample_rows is then
        # [] and detect_header(()) returns has_header=False.
        raise StructuralValidationError(
            "no header row could be detected",
            context={"error_code": errors.NO_HEADER_ROW},
        )

    declared_names = {c.name for c in config.columns}
    header_names = set(header_detection.raw_header)
    # D-21: order-independent, matched by name only. D-22: case-sensitive,
    # exact-string equality, never normalized first.
    missing = declared_names - header_names
    if missing:
        raise StructuralValidationError(
            f"header is missing declared column(s): {sorted(missing)}",
            context={
                "error_code": errors.MISSING_REQUIRED_COLUMN,
                "missing": sorted(missing),
            },
        )
    extra = header_names - declared_names
    if extra:
        raise StructuralValidationError(
            f"header has undeclared column(s): {sorted(extra)}",
            context={
                "error_code": errors.EXTRA_UNEXPECTED_COLUMN,
                "extra": sorted(extra),
            },
        )

    # PASS 2: real read, reopened fresh with the resolved codec/dialect.
    real_stream = _open_raw_stream(file_path)
    text_stream: TextIO = io.TextIOWrapper(
        real_stream, encoding=decode_codec, newline="", errors="strict"
    )
    wrapper = _LineCapturingTextStream(text_stream)
    reader = csv.reader(
        wrapper,
        delimiter=config.csv.delimiter,
        quotechar=config.csv.quotechar,
        doublequote=config.csv.doublequote,
        escapechar=config.csv.escapechar,
    )
    next(reader)  # skip the header row -- already validated above
    return text_stream, _rows_with_raw_line(reader, wrapper), header_detection.raw_header
