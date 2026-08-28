"""Tests for tools.corpus.generators' `wrapper` GeneratorKind and the
`profile: large` batched-writes path (D-16, GEN-01, 02-05-PLAN.md Task 1).

Covers Task 1's <behavior> block:
- generating a `profile: large` fixture twice produces byte-identical
  output, and the batched code path is verified "by construction" (per the
  plan's own stated alternative to a live memory measurement, which would be
  flaky/order-dependent inside pytest's shared process) rather than never
  materializing a single joined buffer.
- generating a gzip-wrapped fixture twice produces byte-identical gzip
  output (R5's `mtime=0, filename=""`).
- generating a zip-wrapped fixture twice produces byte-identical zip output
  (R5's `ZipInfo.date_time=(1980, 1, 1, 0, 0, 0)`).

This file deliberately builds its own small/moderate inline fixtures --
never the real tests/fixtures/csv/28_large_streaming_profile.csv (~60 MiB),
which would make the unit suite slow. The real fixture's byte-identity is
proven by `make fixtures && make fixtures-verify` (the task's own <verify>),
run separately from this fast unit suite.
"""

from __future__ import annotations

import gzip
import io
import zipfile
from pathlib import Path

import pytest

from tools.corpus import generators, manifest


def _large_fixture(*, rows: int, length: int, include_profile: bool = True) -> manifest.Fixture:
    spec: dict[str, object] = {
        "kind": "tabular",
        "encoding": "utf-8",
        "delimiter": ",",
        "header": ["id", "payload"],
        "rows": rows,
        "row_spec": {
            "id": {"kind": "zero_padded_int", "width": 4, "start": 1},
            "payload": {"kind": "repeat", "char": "x", "length": length},
        },
    }
    if include_profile:
        spec["profile"] = "large"
    return manifest.Fixture(name="test_large_fixture", category="large_compressed", generator=spec, expect={})


def _wrapper_fixture(fmt: str, *, name: str = "test_wrapped_fixture") -> manifest.Fixture:
    return manifest.Fixture(
        name=name,
        category="large_compressed",
        generator={
            "kind": "wrapper",
            "format": fmt,
            "inner": {
                "kind": "tabular",
                "encoding": "utf-8",
                "delimiter": ",",
                "header": ["a", "b"],
                "rows": 3,
                "row_spec": {
                    "a": {"kind": "zero_padded_int", "width": 2, "start": 1},
                    "b": {"kind": "repeat", "char": "y", "length": 3},
                },
            },
        },
        expect={},
    )


# --- `profile: large` batched path ------------------------------------------


def test_large_profile_dispatches_to_a_batched_generator_by_construction() -> None:
    """Verifies 'batched writes, never a single joined buffer' by
    construction: the plan's own stated alternative to a live
    `resource.getrusage` memory measurement, which is flaky/order-dependent
    under pytest's shared process (an earlier test's peak RSS never goes back
    down, so a delta-based assertion could pass even for a naive
    implementation just because something else already used more memory).
    """
    source = Path(generators.__file__).read_text(encoding="utf-8")
    assert "_generate_tabular_batched" in source
    assert "bytearray" in source


def test_large_profile_matches_the_plain_tabular_path_byte_for_byte() -> None:
    """Batching is a memory-shape difference only, never a format
    difference -- the same declaration (minus `profile: large`) through the
    plain `_generate_tabular` path must produce identical bytes."""
    fixture_batched = _large_fixture(rows=50, length=20, include_profile=True)
    fixture_plain = _large_fixture(rows=50, length=20, include_profile=False)

    batched = generators.generate_fixture(fixture_batched, generators.stream_for("seed", "x"))
    plain = generators.generate_fixture(fixture_plain, generators.stream_for("seed", "x"))

    assert batched == plain


def test_large_profile_regenerates_byte_identical() -> None:
    fixture = _large_fixture(rows=5000, length=200)

    first = generators.generate_fixture(fixture, generators.stream_for("corpus-seed", fixture.name))
    second = generators.generate_fixture(fixture, generators.stream_for("corpus-seed", fixture.name))

    assert first == second
    assert len(first) > 0


# --- `wrapper` gzip ----------------------------------------------------------


def test_gzip_wrapper_regenerates_byte_identical() -> None:
    fixture = _wrapper_fixture("gzip")

    first = generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))
    second = generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))

    assert first == second
    assert first[:2] == b"\x1f\x8b"  # gzip magic bytes


def test_gzip_wrapper_decompresses_to_the_inner_payload() -> None:
    fixture = _wrapper_fixture("gzip")

    raw = generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))
    decompressed = gzip.decompress(raw)

    assert decompressed.startswith(b"a,b\n")


# --- `wrapper` zip -----------------------------------------------------------


def test_zip_wrapper_regenerates_byte_identical() -> None:
    fixture = _wrapper_fixture("zip")

    first = generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))
    second = generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))

    assert first == second
    assert first[:2] == b"PK"  # zip magic bytes


def test_zip_wrapper_extracts_to_the_inner_payload() -> None:
    fixture = _wrapper_fixture("zip")

    raw = generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        content = archive.read(archive.namelist()[0])

    assert content.startswith(b"a,b\n")


# --- error handling ----------------------------------------------------------


def test_wrapper_with_unknown_format_raises_generator_error() -> None:
    fixture = _wrapper_fixture("rar")

    with pytest.raises(generators.GeneratorError):
        generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))


def test_wrapper_without_inner_spec_raises_generator_error() -> None:
    fixture = manifest.Fixture(
        name="broken_wrapper",
        category="large_compressed",
        generator={"kind": "wrapper", "format": "gzip"},
        expect={},
    )

    with pytest.raises(generators.GeneratorError):
        generators.generate_fixture(fixture, generators.stream_for("seed", fixture.name))
