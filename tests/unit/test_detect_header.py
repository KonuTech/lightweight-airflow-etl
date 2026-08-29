"""Proves the vendored ``csv_processor.detect.header`` module against all 8
``dialect_encoding`` corpus fixtures, Phase 3 Plan 2 Task 3 backing evidence.

Materializes each fixture's bytes directly from the committed
``tests/fixtures/corpus.yaml`` manifest via ``tools.corpus.generators`` --
never depends on ``tests/fixtures/csv/**`` already existing on disk
(gitignored, D-16e).

Runs the FULL detect-once sequence (encoding -> decode -> dialect -> parse
-> header) for each fixture, since ``detect_header`` itself operates on
already-parsed row tuples, not raw text -- exactly the orchestration
``source.py`` (a later plan) will perform. Every one of fixtures 1-8 is
expected to detect a header at row 0 with ``raw_header`` matching that
fixture's own declared ``header:`` list exactly, order preserved.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

from csv_processor.detect.dialect import detect_dialect, to_stdlib_dialect
from csv_processor.detect.encoding import decode_strict, detect_encoding
from csv_processor.detect.header import detect_header

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")

# Fixture name -> its own manifest-declared header list (order preserved).
_EXPECTED_HEADERS: dict[str, tuple[str, ...]] = {
    "01_semicolon_delimiter": ("id", "name", "city"),
    "02_pipe_delimiter": ("id", "name", "city"),
    "03_tab_delimiter": ("id", "name", "city"),
    "04_custom_quotechar": ("id", "full_name", "note"),
    "05_escapechar_no_doublequote": ("id", "quote_text", "note"),
    "06_crlf_terminator": ("id", "name", "city"),
    "07_utf8_bom": ("id", "name", "city"),
    "08_utf16_encoding": ("id", "name", "city"),
}


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def _detected_header_row(name: str) -> tuple[str, ...]:
    """Run the full detect-once-per-file sequence and return the raw header.

    Mirrors Research Pattern 1's resolve-once sequence (encoding -> decode
    -> dialect -> header), the same order ``source.py`` will use.
    """
    raw = _fixture_bytes(name)
    encoding_detection = detect_encoding(raw, contract_encoding=None)
    decoded = decode_strict(raw, encoding_detection)
    dialect_detection = detect_dialect(decoded, contract_delimiter=None)
    stdlib_dialect = to_stdlib_dialect(dialect_detection)
    rows = tuple(tuple(row) for row in csv.reader(io.StringIO(decoded), dialect=stdlib_dialect))
    header_detection = detect_header(rows)
    assert header_detection.has_header is True, (name, header_detection)
    return header_detection.raw_header


def test_all_eight_fixtures_detect_header_at_row_zero_matching_declared_order() -> None:
    for name, expected_header in _EXPECTED_HEADERS.items():
        assert _detected_header_row(name) == expected_header, name


def test_01_semicolon_delimiter_header() -> None:
    assert _detected_header_row("01_semicolon_delimiter") == ("id", "name", "city")


def test_02_pipe_delimiter_header() -> None:
    assert _detected_header_row("02_pipe_delimiter") == ("id", "name", "city")


def test_03_tab_delimiter_header() -> None:
    assert _detected_header_row("03_tab_delimiter") == ("id", "name", "city")


def test_04_custom_quotechar_header() -> None:
    assert _detected_header_row("04_custom_quotechar") == ("id", "full_name", "note")


def test_05_escapechar_no_doublequote_header() -> None:
    assert _detected_header_row("05_escapechar_no_doublequote") == ("id", "quote_text", "note")


def test_06_crlf_terminator_header() -> None:
    assert _detected_header_row("06_crlf_terminator") == ("id", "name", "city")


def test_07_utf8_bom_header() -> None:
    assert _detected_header_row("07_utf8_bom") == ("id", "name", "city")


def test_08_utf16_encoding_header() -> None:
    assert _detected_header_row("08_utf16_encoding") == ("id", "name", "city")
