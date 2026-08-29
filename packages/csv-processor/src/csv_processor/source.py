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

from csv_processor import compression, detect, errors
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
    """Open ``file_path``'s raw byte stream, transparently decompressing if needed.

    Peeks the first 4 bytes to magic-byte-sniff a compression kind
    (D-29/D-30) via ``compression.detect_compression``, then seeks back to
    the start. An uncompressed file (``detect_compression`` returns
    ``None``) returns this SAME already-open, seeked-back handle directly --
    no second ``open()`` call, keeping the uncompressed path byte-for-byte
    identical to every 03-03 test's already-passing behavior. A compressed
    file closes this peek handle and delegates to
    ``compression.open_compressed_stream`` for true streaming decompression
    (D-29's "never extract-to-a-temp-file" requirement).

    No other function in this module changes -- every caller keeps calling
    this function by name, so wiring compression in here is the only change
    03-04 makes to ``source.py``.
    """
    handle = file_path.open("rb")
    magic = handle.read(4)
    handle.seek(0)
    detected = compression.detect_compression(magic)
    if detected is None:
        return handle
    handle.close()
    return compression.open_compressed_stream(file_path, compression=detected)


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


def _uncoverable_tail_indices(
    excluded_indices: set[int], sample_covered_row_count: int
) -> set[int]:
    """Compute the maximal contiguous suffix of ``excluded_indices`` ending
    at (and including) ``sample_covered_row_count`` itself (CR-01).

    Root cause this closes: content re-validation (CR-03) alone cannot
    distinguish a genuine footer/repeated-header row from a genuinely
    malformed data row that also happens to mismatch the header's field
    count, whenever both are swept into the SAME contiguous backward-walk
    ``detect/header.py``'s ``_detect_footer_rows`` produces. 03-08's own
    CR-04 fix protected only the single index equal to
    ``sample_covered_row_count`` -- every OTHER index in the same contiguous
    run remained "eligible" under that single-index comparison and was only
    checked by CR-03's content re-validation, which "confirms" a genuinely
    malformed row exactly as readily as a genuine footer, silently dropping
    it. This helper generalizes CR-04's single-index protection to the full
    contiguous run.

    Args:
        excluded_indices: A SINGLE candidate-index source set (never a
            pre-merged union of multiple sources -- see ``_filtered_rows``'s
            own docstring for why the two candidate sources here must be
            walked separately and unioned only after each walk completes).
        sample_covered_row_count: The sample's own unprovable last row's
            absolute index whenever the sample was truncated --
            ``sample_covered_row_count`` is a COUNT of provably-covered
            rows, so index ``sample_covered_row_count`` itself is the first
            UNPROVABLE one.

    Returns:
        Every index in ``excluded_indices`` that is part of the unbroken
        run ending at ``sample_covered_row_count`` -- each is ineligible for
        exclusion regardless of what content re-validation would otherwise
        conclude about it. Self-terminates safely at index 0 or below (a
        negative index can never be a member of a set of non-negative row
        indices, so no explicit bounds guard is needed) and returns an
        empty set when ``excluded_indices`` is empty or does not touch the
        boundary at all.
    """
    uncoverable: set[int] = set()
    idx = sample_covered_row_count
    while idx in excluded_indices:
        uncoverable.add(idx)
        idx -= 1
    return uncoverable


def _filtered_rows(
    paired_rows: Iterator[tuple[list[str], str]],
    *,
    start_index: int,
    excluded_indices: set[int],
    footer_row_indices: set[int],
    repeated_header_row_indices: set[int],
    header_field_count: int,
    raw_header: tuple[str, ...],
    sample_covered_row_count: int,
) -> Iterator[tuple[list[str], str]]:
    """Exclude only REAL footer/repeated-header rows from ``paired_rows`` (CR-03/CR-04).

    ``excluded_indices`` (footer rows + repeated-header rows) is computed by
    ``detect_header()`` against only a bounded 64 KiB HEAD SAMPLE
    (``SAMPLE_BYTES``) of the file, not the real file this generator reads.
    On any file larger than that sample, the row whose bytes straddle the
    sample's byte cutoff is truncated MID-ROW WITHIN THE SAMPLE ONLY --
    ``detect_header()``'s footer-scoring walk sees a malformed field count
    for that truncated copy and flags its absolute index as "footer" or
    "repeated-header", even though the SAME row, read here in full from the
    real file, is a perfectly well-formed data row (03-REVIEW.md's Critical
    finding: a clean 6,000-row CSV losing exactly 1 row with zero error
    surfaced anywhere). 03-06's original version (CR-02) trusted
    ``excluded_indices`` membership alone and silently dropped that row.

    (CR-03) re-validates every candidate exclusion against the REAL row at
    that position before actually excluding it: a row is only skipped when
    its real content independently fails the SAME criterion
    ``detect_header()`` itself would apply here (this module never passes
    ``footer_patterns``/``skip_footer_rows`` to ``detect_header()``, so the
    only footer criterion in play is a field-count mismatch) -- real field
    count differs from the header's own field count, or real values exactly
    equal the header's own raw values (repeated-header). A row whose real
    content actually matches the header proves the sample-derived flag was
    a truncation artifact, not a genuine footer/repeat, and passes through
    untouched.

    (CR-04) Content re-validation ALONE cannot distinguish a genuinely
    malformed data row from a real footer/repeated-header row when both
    happen to occupy the sample's own arbitrary tail position:
    ``detect/header.py``'s ``_detect_footer_rows`` always walks backward
    from the LAST row of whatever ``rows`` it receives -- the sample's
    truncation cutoff, not the real file's tail, for any file exceeding
    ``SAMPLE_BYTES``. A genuinely malformed row that happens to land there
    trivially "confirms" its own exclusion under CR-03's re-validated
    criterion too (03-REVIEW.md's CR-04 reproduction: a ~125 KB file with
    one malformed row at the sample's tail-adjacent position vanished from
    both ``valid_rows`` and ``invalid_rows``). 03-08's fix layered in front
    of CR-03 protected only the SINGLE index equal to
    ``sample_covered_row_count`` -- flows through to PASS 2's ordinary
    per-row validation.

    (CR-01) A contiguous run of 2+ sample-derived candidates ending at the
    boundary -- not just the single last one CR-04 protected -- must ALL be
    treated as coverage-ineligible: ``_detect_footer_rows``'s own backward
    walk chains contiguous field-count mismatches together (e.g. an ordinary
    trailing blank line immediately adjacent to a genuinely-truncated row),
    and content re-validation cannot independently distinguish a genuine
    footer from a genuinely malformed row caught in the same chain. The fix:
    ``_uncoverable_tail_indices()`` computes the maximal contiguous suffix of
    a candidate-index set ending at (and including) ``sample_covered_row_count``
    itself, and every index in that suffix is ineligible for exclusion,
    checked BEFORE CR-03's content re-validation, exactly generalizing
    CR-04's single-index protection to the full run.

    The uncoverable-tail computation is done per-source-then-unioned, never
    union-then-walk: ``footer_row_indices`` (``_detect_footer_rows``'s own
    contiguous backward walk) and ``repeated_header_row_indices``
    (``_detect_repeated_header_rows``'s unbounded full-scan, structurally
    unrelated to boundary adjacency) are two independent detectors over the
    SAME ``data_rows`` whose candidate indices can be numerically adjacent by
    pure coincidence, never by any shared walk or ordering guarantee. Merging
    them into a single set BEFORE computing the contiguous-run walk would let
    one source's genuine boundary-touching run swallow the OTHER source's
    unrelated interior candidate merely because the two happen to sit next to
    each other -- so ``_uncoverable_tail_indices()`` is called TWICE, once per
    source, each independently anchored at ``sample_covered_row_count``, and
    only the two RESULTING sets are unioned. A genuine interior
    repeated-header row (G-03-2) is never stripped of its exclusion-
    eligibility merely because it happens to sit next to an unrelated,
    independently-flagged boundary-touching footer/malformed-row candidate
    from the other source. A genuine, small, within-sample preamble/footer/
    repeated-header row (G-03-2) that does not touch the boundary at all is
    always well within ``sample_covered_row_count``'s provably-covered range
    and is still excluded, unchanged.

    Args:
        paired_rows: The ``(row, raw_line)`` pairs remaining after PASS 2's
            preamble/header skip.
        start_index: The absolute row index of the first item in
            ``paired_rows``. Matches ``detect_header()``'s own
            absolute-row-index convention exactly, since PASS 2 reopens and
            re-reads the identical file from byte 0.
        excluded_indices: Candidate absolute row indices (footer rows +
            repeated-header rows, both computed by ``detect_header()``
            against only the bounded sample) to re-validate before
            excluding. Used unchanged for the coarse per-row candidacy
            membership check -- the two source sets below are what feed the
            uncoverable-tail computation instead.
        footer_row_indices: (CR-01) ``detect_header()``'s own
            ``footer_row_indices`` -- one of the two candidate sources,
            walked separately from ``repeated_header_row_indices`` (see
            above).
        repeated_header_row_indices: (CR-01) ``detect_header()``'s own
            ``repeated_header_row_indices`` -- the other candidate source,
            walked separately from ``footer_row_indices`` (see above).
        header_field_count: The real header's own field count
            (``len(raw_header)``) -- a real row's field count differing
            from this is the re-validated footer criterion.
        raw_header: The real header's own field values -- a real row whose
            values exactly equal this is the re-validated repeated-header
            criterion.
        sample_covered_row_count: (CR-04/CR-01) The count of rows PASS 1's
            sample bytes provably read in full -- the anchor both
            ``_uncoverable_tail_indices()`` calls start their backward walk
            from, checked BEFORE the content re-validation above.

    Yields:
        Every ``(row, raw_line)`` pair except those whose absolute index is
        in ``excluded_indices``, NOT part of either source's
        uncoverable-tail run, AND independently confirmed, against its own
        real content, to be footer-shaped or a repeated header.
    """
    uncoverable_tail = _uncoverable_tail_indices(
        footer_row_indices, sample_covered_row_count
    ) | _uncoverable_tail_indices(repeated_header_row_indices, sample_covered_row_count)
    for absolute_index, (row, raw_line) in enumerate(paired_rows, start=start_index):
        if absolute_index in excluded_indices and absolute_index not in uncoverable_tail:
            is_footer_shaped = len(row) != header_field_count
            is_repeated_header = tuple(row) == raw_header
            if is_footer_shaped or is_repeated_header:
                continue
        yield row, raw_line


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
        sample = raw_stream.read(SAMPLE_BYTES + 1)
    finally:
        raw_stream.close()

    # WR-01: read one byte past SAMPLE_BYTES and use its actual presence to
    # determine truncation -- `len(sample) == SAMPLE_BYTES` alone conflates
    # "we read exactly the sample-size worth of bytes" with "there is more
    # file beyond the sample": a file whose real (decompressed) size is
    # exactly SAMPLE_BYTES also reads exactly SAMPLE_BYTES bytes and hits
    # true EOF, with nothing left. Works uniformly for compressed and
    # uncompressed streams (both are plain binary file-like objects
    # supporting `.read(n)`; no stream-type-specific branching needed). Trim
    # back to SAMPLE_BYTES immediately so every downstream detector
    # (encoding cross-check, dialect detection, `sample_rows` construction)
    # sees at most SAMPLE_BYTES bytes exactly as before this change.
    sample_was_truncated = len(sample) > SAMPLE_BYTES
    if sample_was_truncated:
        sample = sample[:SAMPLE_BYTES]

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

    # CR-04: `sample_was_truncated` (computed above, WR-01) is true whenever
    # there is more file beyond what PASS 1 read -- the LAST row
    # `csv.reader` parsed from the sample can then never be proven complete
    # (it may be a genuine, whole row that merely ends near the cutoff, or a
    # truncated fragment; either way, it cannot be trusted as the file's
    # real tail). Every OTHER row in `sample_rows` IS provably complete,
    # since each is followed by more parsed content confirming it ended
    # before the truncation point. When the sample was NOT truncated, the
    # sample IS the entire file, so every row including the last is
    # provably complete.
    sample_covered_row_count = (
        len(sample_rows) - 1 if sample_was_truncated and sample_rows else len(sample_rows)
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
    required_names = {c.name for c in config.columns if c.required}
    header_names = set(header_detection.raw_header)
    # D-21: order-independent, matched by name only. D-22: case-sensitive,
    # exact-string equality, never normalized first.
    # CR-01: only a column actually declared `required: true` can trigger
    # MISSING_REQUIRED_COLUMN -- a `required: false` column (e.g.
    # customers.json's own signup_country) may be genuinely absent. `extra`
    # below still compares against the FULL `declared_names` set, never
    # `required_names` -- a present, correctly-named optional column must
    # never be flagged EXTRA_UNEXPECTED_COLUMN.
    missing = required_names - header_names
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
    # CR-02: skip every preamble row AND the header row itself -- not just
    # one hardcoded row. `header_row_index` is guaranteed non-None here (the
    # `has_header` check above already raised otherwise).
    for _ in range(header_detection.header_row_index + 1):  # type: ignore[operator]
        next(reader)
    excluded_indices = set(header_detection.footer_row_indices) | set(
        header_detection.repeated_header_row_indices
    )
    # CR-03: `excluded_indices` is only a sample-derived CANDIDATE set --
    # `_filtered_rows()` re-validates each candidate against the real row at
    # that position before excluding it (see its own docstring).
    return (
        text_stream,
        _filtered_rows(
            _rows_with_raw_line(reader, wrapper),
            start_index=header_detection.header_row_index + 1,  # type: ignore[operator]
            excluded_indices=excluded_indices,
            footer_row_indices=set(header_detection.footer_row_indices),
            repeated_header_row_indices=set(header_detection.repeated_header_row_indices),
            header_field_count=len(header_detection.raw_header),
            raw_header=header_detection.raw_header,
            sample_covered_row_count=sample_covered_row_count,
        ),
        header_detection.raw_header,
    )
