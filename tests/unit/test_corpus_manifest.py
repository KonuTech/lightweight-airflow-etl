"""Tests for tools.corpus's manifest/generators/digests mechanism (D-16, GEN-01).

Covers Task 2's <behavior> block exactly:
- stream_for() per-fixture RNG derivation (R1) — different fixtures diverge,
  the same fixture reproduces its own sequence identically.
- load_manifest() success path on a minimal one-fixture manifest.
- load_manifest() rejects an unrecognized fixture-level key (schema-level
  `extra` forbidden, matching the reference repo's own validation discipline).
- digests.format_digests()'s exact sha256sum-compatible two-space format.
- digests.sha256_file()'s chunked read matches a whole-file hashlib digest.

This file deliberately builds its own small inline/temp-file corpus.yaml —
never the real tests/fixtures/corpus.yaml, which Task 3 authors.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from tools.corpus import digests, generators, manifest


# --- stream_for() — R1 per-fixture RNG derivation ---------------------------


def test_stream_for_diverges_across_fixture_names() -> None:
    stream_a = generators.stream_for("seed1", "fixture_a")
    stream_b = generators.stream_for("seed1", "fixture_b")

    values_a = [stream_a.random() for _ in range(5)]
    values_b = [stream_b.random() for _ in range(5)]

    assert values_a != values_b


def test_stream_for_is_deterministic_for_the_same_name() -> None:
    first_call = [generators.stream_for("seed1", "fixture_a").random() for _ in range(5)]
    second_call = [generators.stream_for("seed1", "fixture_a").random() for _ in range(5)]

    assert first_call == second_call


# --- load_manifest() success path -------------------------------------------


_MINIMAL_MANIFEST = """\
version: 1
master_seed: "test-seed"
fixtures:
  - name: "01_minimal"
    category: "dialect_encoding"
    generator:
      kind: literal
      encoding: utf-8
      content: "a,b\\n1,2\\n"
    expect:
      reason: "minimal literal fixture parses as a two-column CSV"
"""


def test_load_manifest_returns_one_fixture_matching_the_yaml(tmp_path: Path) -> None:
    manifest_path = tmp_path / "corpus.yaml"
    manifest_path.write_text(_MINIMAL_MANIFEST, encoding="utf-8")

    fixtures = manifest.load_manifest(manifest_path)

    assert len(fixtures) == 1
    fixture = fixtures[0]
    assert fixture.name == "01_minimal"
    assert fixture.generator["kind"] == "literal"


# --- load_manifest() rejects unrecognized keys ------------------------------


_MANIFEST_WITH_UNKNOWN_FIXTURE_KEY = """\
version: 1
master_seed: "test-seed"
fixtures:
  - name: "01_minimal"
    category: "dialect_encoding"
    generator:
      kind: literal
      encoding: utf-8
      content: "a,b\\n1,2\\n"
    unexpected_field: "this key does not exist in the schema"
    expect:
      reason: "should never load"
"""


def test_load_manifest_rejects_unrecognized_top_level_fixture_key(tmp_path: Path) -> None:
    manifest_path = tmp_path / "corpus.yaml"
    manifest_path.write_text(_MANIFEST_WITH_UNKNOWN_FIXTURE_KEY, encoding="utf-8")

    with pytest.raises(manifest.ManifestError):
        manifest.load_manifest(manifest_path)


def test_load_manifest_rejects_unrecognized_root_key(tmp_path: Path) -> None:
    manifest_path = tmp_path / "corpus.yaml"
    manifest_path.write_text(
        _MINIMAL_MANIFEST.replace(
            'master_seed: "test-seed"',
            'master_seed: "test-seed"\nunexpected_root_field: "nope"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(manifest.ManifestError):
        manifest.load_manifest(manifest_path)


def test_load_manifest_never_uses_the_unsafe_yaml_loader() -> None:
    """Policy check backing T-02-04: only yaml.safe_load may be CALLED.

    Scans actual code lines only (skipping the module's own prose docstring,
    which legitimately names ``yaml.load``/``yaml.unsafe_load`` to explain why
    neither is used) so the check cannot be satisfied by deleting the
    rationale instead of the call.
    """
    source_lines = Path(manifest.__file__).read_text(encoding="utf-8").splitlines()
    code_lines = [line for line in source_lines if not line.strip().startswith(('"""', "``", "#"))]
    code_lines = [line for line in code_lines if "``yaml" not in line]

    assert any("yaml.safe_load(" in line for line in code_lines)
    assert not any("yaml.load(" in line for line in code_lines)
    assert not any("yaml.unsafe_load(" in line for line in code_lines)


# --- digests.format_digests() -----------------------------------------------


def test_format_digests_matches_sha256sum_two_space_format() -> None:
    result = digests.format_digests({"tests/fixtures/csv/01_x.csv": "abc123"})

    assert result == "abc123  tests/fixtures/csv/01_x.csv\n"


# --- digests.sha256_file() chunked read -------------------------------------


def test_sha256_file_chunked_read_matches_whole_file_hash(tmp_path: Path) -> None:
    sample = tmp_path / "sample.bin"
    sample.write_bytes(b"some sample bytes for hashing" * 100)

    expected = hashlib.sha256(sample.read_bytes()).hexdigest()
    actual = digests.sha256_file(sample)

    assert actual == expected
