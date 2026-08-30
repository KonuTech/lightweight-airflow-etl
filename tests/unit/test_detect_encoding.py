"""Proves the vendored ``csv_processor.detect.encoding`` module against the
``dialect_encoding`` corpus category's BOM/wide-encoding fixtures (7-8),
Phase 3 Plan 2 Task 3 backing evidence.

Materializes each fixture's bytes directly from the committed
``tests/fixtures/corpus.yaml`` manifest via ``tools.corpus.generators`` --
never depends on ``tests/fixtures/csv/**`` already existing on disk
(gitignored, D-16e).

Fixture 7 proves a UTF-8 byte-order mark is detected deterministically
(``source="bom"``, confidence 1.0) and stripped before decoding (D-26) --
the decoded header's first field must be exactly ``"id"``, never
``"\\ufeffid"``. Fixture 8 proves a bare UTF-16 file (Python's own
``"utf-16"`` codec embeds its own byte-order mark) is recognized as UTF-16,
not misread as UTF-8/Latin-1 garbage, and decodes to the exact plain-ASCII
content the fixture declares.
"""

from __future__ import annotations

import codecs
from pathlib import Path

from csv_processor.detect.encoding import decode_strict, detect_encoding

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def test_07_utf8_bom_detected_and_stripped_before_decode() -> None:
    sample = _fixture_bytes("07_utf8_bom")
    detection = detect_encoding(sample, contract_encoding=None)

    assert detection.source == "bom"
    assert detection.confidence == 1.0
    # "utf-8-sig" is the codec that strips the BOM on decode -- confirm it
    # normalizes to a real UTF-8-family codec name, not an unrelated one.
    assert codecs.lookup(detection.encoding).name == "utf-8-sig"

    decoded = decode_strict(sample, detection)
    header_line = decoded.split("\n", 1)[0]
    first_field = header_line.split(",", 1)[0]
    assert first_field == "id"
    assert first_field != "﻿id"
    assert "﻿" not in decoded


def test_08_utf16_encoding_detected_and_decodes_exact_ascii_content() -> None:
    sample = _fixture_bytes("08_utf16_encoding")
    detection = detect_encoding(sample, contract_encoding=None)

    # UTF-16 always embeds its own byte-order mark (Python's "utf-16" codec
    # always writes one) -- so this is a deterministic "bom" detection, not
    # a probabilistic "detected" one, but either source is acceptable here
    # per the plan's own wording; what matters is the codec name and the
    # decoded content.
    assert detection.source in ("bom", "detected")
    assert codecs.lookup(detection.encoding).name == "utf-16"
    assert detection.confidence >= 0.5

    decoded = decode_strict(sample, detection)
    assert decoded == "id,name,city\n1,omar,tunis\n2,priya,pune\n"
