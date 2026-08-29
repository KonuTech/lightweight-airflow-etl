"""Proves ``csv_processor.compression``'s magic-byte detection + streaming
gzip/zip open (D-29/D-30/D-33, 03-04-PLAN.md Task 1) against corpus fixtures
28 (large streaming, uncompressed), 29 (gzip-wrapped), 30 (zip-wrapped), plus
two synthetic cases the corpus doesn't cover: a multi-member zip archive and
an artificially tiny decompression-bomb ceiling.

``detect_compression`` never inspects a filename or extension -- only the
first few bytes of ``sample``, matching D-30's pattern-agnostic requirement.
"""

from __future__ import annotations

import gzip
import io
import zipfile
from pathlib import Path

import pytest

from csv_processor.compression import detect_compression, open_compressed_stream
from csv_processor.errors import FileInspectionError

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import Fixture, load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")


def _load_fixture(name: str) -> Fixture:
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    return next(f for f in manifest.fixtures if f.name == name)


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def _inner_payload_bytes(wrapper_fixture: Fixture) -> bytes:
    """Reproduce a wrapper fixture's own INNER (pre-compression) bytes.

    ``generators._generate_wrapper`` builds this exact same
    ``Fixture(name=wrapper.name, generator=wrapper.generator["inner"], ...)``
    and feeds it the wrapper's own private RNG stream (derived from the
    wrapper fixture's own name, R1) before ever compressing anything -- since
    nothing else consumes that stream first, re-deriving it here and
    generating the inner spec independently reproduces byte-identical
    content, without needing to gzip/zip-decompress the wrapper's own output
    to get it.
    """
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    inner_fixture = Fixture(
        name=wrapper_fixture.name,
        category=wrapper_fixture.category,
        generator=wrapper_fixture.generator["inner"],
        expect=wrapper_fixture.expect,
    )
    rng = stream_for(manifest.master_seed, wrapper_fixture.name)
    return generate_fixture(inner_fixture, rng)


# --- detect_compression: magic-byte sniffing, corpus fixtures ------------


def test_detect_compression_classifies_fixture_29_gzip_magic_bytes() -> None:
    content = _fixture_bytes("29_gzip_wrapped_valid_file")

    assert content[:2] == b"\x1f\x8b"
    assert detect_compression(content) == "gzip"


def test_detect_compression_classifies_fixture_30_zip_magic_bytes() -> None:
    content = _fixture_bytes("30_zip_wrapped_valid_file")

    assert content[:4] == b"PK\x03\x04"
    assert detect_compression(content) == "zip"


def test_detect_compression_returns_none_for_fixture_28_plain_csv() -> None:
    """Fixture 28 (large_streaming_profile) is a plain, uncompressed CSV --
    its own first bytes must never be misclassified as gzip/zip."""
    content = _fixture_bytes("28_large_streaming_profile")

    assert detect_compression(content[:4]) is None


def test_detect_compression_never_inspects_extension_or_filename() -> None:
    """detect_compression's only parameter is `sample: bytes` -- a plain CSV
    sample is never reclassified based on some out-of-band filename."""
    plain_csv_sample = b"customer_id,name,country\n"

    assert detect_compression(plain_csv_sample) is None


# --- open_compressed_stream: streaming decompression, byte-exact ---------


def test_gzip_stream_decompresses_to_exact_inner_payload(tmp_path: Path) -> None:
    fixture = _load_fixture("29_gzip_wrapped_valid_file")
    gz_path = tmp_path / "29.csv.gz"
    gz_path.write_bytes(_fixture_bytes(fixture.name))

    stream = open_compressed_stream(gz_path, compression="gzip")
    try:
        decompressed = stream.read()
    finally:
        stream.close()

    assert decompressed == _inner_payload_bytes(fixture)


def test_zip_stream_decompresses_to_exact_inner_payload(tmp_path: Path) -> None:
    fixture = _load_fixture("30_zip_wrapped_valid_file")
    zip_path = tmp_path / "30.csv.zip"
    zip_path.write_bytes(_fixture_bytes(fixture.name))

    stream = open_compressed_stream(zip_path, compression="zip")
    try:
        decompressed = stream.read()
    finally:
        stream.close()

    assert decompressed == _inner_payload_bytes(fixture)


def test_uncompressed_passthrough_returns_identical_bytes(tmp_path: Path) -> None:
    plain_path = tmp_path / "plain.csv"
    plain_path.write_bytes(b"customer_id,name\n001,alice\n")

    stream = open_compressed_stream(plain_path, compression=None)
    try:
        content = stream.read()
    finally:
        stream.close()

    assert content == b"customer_id,name\n001,alice\n"


# --- D-33: exactly one member, else a structural reject -------------------


def test_zip_archive_with_two_members_raises_before_opening_any_member(tmp_path: Path) -> None:
    zip_path = tmp_path / "two_members.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("payload_one.csv", "id,name\n1,alice\n")
        archive.writestr("payload_two.csv", "id,name\n2,bob\n")
    zip_path.write_bytes(buffer.getvalue())

    with pytest.raises(FileInspectionError) as exc_info:
        open_compressed_stream(zip_path, compression="zip")

    assert exc_info.value.context["error_code"] == "CORRUPTED_ARCHIVE"
    assert exc_info.value.context["member_count"] == 2


def test_zip_archive_with_zero_members_raises(tmp_path: Path) -> None:
    zip_path = tmp_path / "empty.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w"):
        pass
    zip_path.write_bytes(buffer.getvalue())

    with pytest.raises(FileInspectionError) as exc_info:
        open_compressed_stream(zip_path, compression="zip")

    assert exc_info.value.context["error_code"] == "CORRUPTED_ARCHIVE"
    assert exc_info.value.context["member_count"] == 0


def test_corrupted_zip_archive_raises_corrupted_archive(tmp_path: Path) -> None:
    zip_path = tmp_path / "corrupted.zip"
    zip_path.write_bytes(b"not a real zip archive at all")

    with pytest.raises(FileInspectionError) as exc_info:
        open_compressed_stream(zip_path, compression="zip")

    assert exc_info.value.context["error_code"] == "CORRUPTED_ARCHIVE"


# --- T-03-09: decompression-bomb ceiling, proven with a real tiny cap -----


def test_decompression_bomb_ceiling_trips_on_oversized_gzip_stream(tmp_path: Path) -> None:
    """A synthetic gzip payload comfortably larger than an artificially tiny
    ``max_decompressed_bytes`` must raise partway through a full ``read()``
    to EOF, never silently returning truncated data as if it were complete.

    Proof the ceiling is load-bearing, not vacuous: with the guard's
    ``self._bytes_read > self._max_decompressed_bytes`` check removed (or
    the ceiling raised above the payload's true size), this exact test would
    instead observe a *successful* 1000-byte `read()`, no exception raised --
    the tiny 100-byte ceiling below is what turns that into a required
    failure.
    """
    payload = b"x" * 1000  # far larger than the 100-byte ceiling below
    gz_path = tmp_path / "bomb.csv.gz"
    gz_path.write_bytes(gzip.compress(payload))

    stream = open_compressed_stream(gz_path, compression="gzip", max_decompressed_bytes=100)
    try:
        with pytest.raises(FileInspectionError) as exc_info:
            stream.read()
    finally:
        stream.close()

    assert exc_info.value.context["error_code"] == "DECOMPRESSION_BOMB_EXCEEDED"


def test_decompression_bomb_ceiling_allows_a_payload_under_the_limit(tmp_path: Path) -> None:
    """Negative control: a payload genuinely under the ceiling must read cleanly."""
    payload = b"y" * 50
    gz_path = tmp_path / "small.csv.gz"
    gz_path.write_bytes(gzip.compress(payload))

    stream = open_compressed_stream(gz_path, compression="gzip", max_decompressed_bytes=100)
    try:
        content = stream.read()
    finally:
        stream.close()

    assert content == payload
