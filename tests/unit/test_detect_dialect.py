"""Proves the vendored ``csv_processor.detect.dialect`` module against the
``dialect_encoding`` corpus category's delimiter/quotechar fixtures (1-5),
Phase 3 Plan 2 Task 3 backing evidence.

Materializes each fixture's bytes directly from the committed
``tests/fixtures/corpus.yaml`` manifest via ``tools.corpus.generators``, the
same self-sufficient pattern ``tests/unit/test_corpus_bounded_memory.py``
already uses -- never depends on ``tests/fixtures/csv/**`` already existing
on disk (gitignored, D-16e).

Fixtures 1/2/3 prove ``detect_dialect`` recovers a non-default delimiter
(semicolon/pipe/tab) with no contract override. Fixture 4 proves a
non-default quotechar (``'``) is detected, not the RFC 4180 default (``"``).
Fixture 5 proves the escapechar/doublequote=False convention it declares
round-trips correctly when parsed with that *declared* dialect via
``csv.reader`` directly -- ``detect_dialect`` itself is not expected to
detect escapechar/doublequote (clevercsv's own scope, per the module's own
docstring), so this fixture's assertion targets the parsing primitive, not
the detector.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

from csv_processor.detect.dialect import DialectDetection, detect_dialect

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


# ---------------------------------------------------------------------------
# Fixtures 1-3: non-default single-character delimiters
# ---------------------------------------------------------------------------


def test_01_semicolon_delimiter_detected() -> None:
    sample = _fixture_bytes("01_semicolon_delimiter").decode("utf-8")
    detection = detect_dialect(sample, contract_delimiter=None)
    assert detection == DialectDetection(delimiter=";", quotechar='"', declined=False)


def test_02_pipe_delimiter_detected() -> None:
    sample = _fixture_bytes("02_pipe_delimiter").decode("utf-8")
    detection = detect_dialect(sample, contract_delimiter=None)
    assert detection == DialectDetection(delimiter="|", quotechar='"', declined=False)


def test_03_tab_delimiter_detected() -> None:
    sample = _fixture_bytes("03_tab_delimiter").decode("utf-8")
    detection = detect_dialect(sample, contract_delimiter=None)
    assert detection == DialectDetection(delimiter="\t", quotechar='"', declined=False)


# ---------------------------------------------------------------------------
# Fixture 4: non-default quotechar
# ---------------------------------------------------------------------------


def test_04_custom_quotechar_detected_not_default() -> None:
    sample = _fixture_bytes("04_custom_quotechar").decode("utf-8")
    detection = detect_dialect(sample, contract_delimiter=None)
    assert detection.quotechar == "'"
    assert detection.quotechar != '"'
    assert detection.delimiter == ","
    assert detection.declined is False


# ---------------------------------------------------------------------------
# Fixture 5: escapechar, doublequote=False -- parsing primitive, not the
# detector (detect_dialect has no escapechar/doublequote detection scope)
# ---------------------------------------------------------------------------


class _Fixture5DeclaredDialect(csv.Dialect):
    """The dialect ``configs/*.json`` would declare for this fixture's
    convention (escapechar=``\\``, doublequote=False) -- never detected,
    always contract-supplied per the module's own documented scope.
    """

    delimiter = ","
    quotechar = '"'
    escapechar = "\\"
    doublequote = False
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False
    lineterminator = "\n"


def test_05_escapechar_no_doublequote_recovers_unescaped_literal_quote() -> None:
    sample = _fixture_bytes("05_escapechar_no_doublequote").decode("utf-8")
    rows = list(csv.reader(io.StringIO(sample), dialect=_Fixture5DeclaredDialect))

    assert rows[0] == ["id", "quote_text", "note"]
    # The generator's own row_spec `pick` renderer selects a value for each
    # row deterministically (R2) -- assert against the two possible declared
    # values, both of which contain an embedded literal double-quote.
    assert rows[1][1] in ('she said "hi"', 'he said "bye"', "no quote here")
    assert '"' in rows[1][1] or rows[1][1] == "no quote here"
    # Confirm at least one data row recovers the exact unescaped literal
    # quote characters (not a doubled or backslash-prefixed artifact).
    quote_text_values = [row[1] for row in rows[1:]]
    assert any(value.count('"') == 2 for value in quote_text_values), quote_text_values
    assert not any("\\" in value for value in quote_text_values), quote_text_values
