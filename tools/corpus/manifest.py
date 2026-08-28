"""The validated corpus manifest model (D-16, GEN-01).

``tests/fixtures/corpus.yaml`` is the only untrusted input this package
deserializes, so it is parsed with the safe YAML loader and nothing else.
Loading it with ``yaml.load``/``yaml.unsafe_load`` would allow arbitrary
Python object construction from the file — a straightforward deserialization
vulnerability (threat T-02-04, ASVS V5) — even though the manifest itself is
developer-authored and version-controlled. Ported (Tier B: read the
algorithm, adapt scope; never copy verbatim) from
``/home/user/projects/airflow-platform/tools/corpus/manifest.py``, whose own
module docstring states the same rationale.

This project's own ``Fixture`` shape is deliberately smaller than the
reference repo's: rather than one flat dataclass with a field per generator
kind's every possible option, ``generator`` stays a single ``dict`` keyed by
``kind`` (the kind-specific config lives inside it). This project's own
validation domain is structural/type/nullability only (per CLAUDE.md) and its
manifest tops out at ~25-30 fixtures across five categories (02-RESEARCH.md's
"Recommended scoped fixture category list") — nowhere near the reference
repo's 69-fixture, five-generator-kind, wrapper/multipart/splice/BOM-mid-file
surface, so a flat 20-plus-field dataclass buys nothing here but
maintenance cost. ``generators.py`` is the only other module that reads
``fixture.generator``'s contents, so the two files agree on the shape by
construction.

The ``expect:`` block stays a permissive ``dict[str, Any]`` (D-16d) — no
fixed ``error_code`` vocabulary is declared here. Phase 3 (ENGINE-06) owns
that vocabulary; pre-locking it here would force Phase 3 into whatever this
phase happened to guess.

Every fixture-level and root-level key is checked against an explicit allow
list — an unrecognized key raises immediately rather than being silently
ignored, matching the reference repo's own hand-written validation
discipline (a manifest that quietly ignores a misspelled field generates a
corpus that quietly means something else).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

# PyYAML 6.0.3 ships no `py.typed`. Every value `yaml.safe_load` returns is
# re-validated by hand below before it reaches a typed model, so the untyped
# boundary this import crosses is one line wide.
import yaml  # type: ignore[import-untyped]

GeneratorKind = Literal["tabular", "literal", "literal_unicode", "wrapper", "multipart"]
RowSpecKind = Literal["zero_padded_int", "pick", "decimal", "repeat"]

_GENERATOR_KINDS: Final[tuple[str, ...]] = (
    "tabular",
    "literal",
    "literal_unicode",
    "wrapper",
    "multipart",
)
_ROW_SPEC_KINDS: Final[tuple[str, ...]] = ("zero_padded_int", "pick", "decimal", "repeat")

# R7: declared in schema order, never a bare set, so any "known keys" hint in
# an error message is independent of set/dict iteration order.
_ROOT_KEY_ORDER: Final[tuple[str, ...]] = ("version", "master_seed", "fixtures")
_FIXTURE_KEY_ORDER: Final[tuple[str, ...]] = ("name", "category", "generator", "expect")

_SUPPORTED_VERSION: Final = 1


class ManifestError(ValueError):
    """A manifest is malformed, self-contradictory, or unsupported.

    Raised at load time so a corrupt manifest fails loudly instead of
    producing a corrupt corpus.
    """


@dataclass(frozen=True, slots=True)
class Fixture:
    """One declared fixture: how to build it and what it is expected to mean.

    Attributes:
        name: File name written under the corpus output directory.
        category: Fixture grouping (e.g. ``"dialect_encoding"``), matching
            02-RESEARCH.md's scoped category list.
        generator: Kind-specific generation config, keyed by ``"kind"``
            (one of ``GeneratorKind``). ``tools/corpus/generators.py``
            dispatches on this dict's ``"kind"`` entry.
        expect: The fixture's declared meaning. Permissive by design (D-16d)
            — no fixed ``error_code`` vocabulary here.
    """

    name: str
    category: str
    generator: dict[str, Any]
    expect: dict[str, Any]


@dataclass(frozen=True, slots=True)
class Manifest:
    """The whole corpus specification.

    Attributes:
        version: Manifest schema version.
        master_seed: Root of every fixture's derived random stream (R1).
        fixtures: Fixtures in declared order — never sorted, never re-ordered.
    """

    version: int
    master_seed: str
    fixtures: tuple[Fixture, ...]


def load_manifest(path: Path | str) -> list[Fixture]:
    """Load and fully validate a corpus manifest, returning its fixtures.

    Args:
        path: Path to the manifest YAML file.

    Returns:
        The validated fixtures, in declared order (R7).

    Raises:
        ManifestError: If the file is unreadable, is not valid safe-YAML, or
            violates any schema rule.
    """
    return list(load_manifest_with_seed(path).fixtures)


def load_manifest_with_seed(path: Path | str) -> Manifest:
    """Load and fully validate a corpus manifest, keeping its master seed.

    ``tools/corpus/__main__.py`` needs ``master_seed`` (to derive each
    fixture's private RNG via ``generators.stream_for``) but Task 2's tested
    public contract for ``load_manifest`` is ``list[Fixture]`` — this is the
    one extra entry point that keeps both true without parsing the file twice
    from the CLI's perspective.

    Args:
        path: Path to the manifest YAML file.

    Returns:
        The validated manifest, fixtures in declared order (R7).

    Raises:
        ManifestError: If the file is unreadable, is not valid safe-YAML, or
            violates any schema rule.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - filesystem failure
        msg = f"{path}: cannot read manifest: {exc}"
        raise ManifestError(msg) from exc
    return _parse_manifest(text, source=str(path))


def _parse_manifest(text: str, *, source: str) -> Manifest:
    try:
        # safe_load only: never `yaml.load`, never a custom Loader that can
        # construct arbitrary Python objects (threat T-02-04, ASVS V5).
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        msg = f"{source}: not a valid safe-YAML document: {exc}"
        raise ManifestError(msg) from exc

    if not isinstance(raw, dict):
        msg = f"{source}: manifest root must be a mapping"
        raise ManifestError(msg)
    _reject_extra_keys(raw, _ROOT_KEY_ORDER, source)

    version = raw.get("version")
    if version != _SUPPORTED_VERSION:
        msg = f"{source}: manifest version {version!r} is unsupported (want {_SUPPORTED_VERSION})"
        raise ManifestError(msg)

    master_seed = raw.get("master_seed")
    if not isinstance(master_seed, str) or not master_seed:
        msg = f"{source}: master_seed must be a non-empty string"
        raise ManifestError(msg)

    raw_fixtures = raw.get("fixtures")
    if not isinstance(raw_fixtures, list) or not raw_fixtures:
        msg = f"{source}: fixtures must be a non-empty list"
        raise ManifestError(msg)

    fixtures: list[Fixture] = []
    seen: set[str] = set()
    # R7: declared order. Never a set, never sorted.
    for index, entry in enumerate(raw_fixtures):
        fixture = _parse_fixture(entry, index=index, source=source)
        if fixture.name in seen:
            msg = f"{source}: duplicate fixture name {fixture.name!r}"
            raise ManifestError(msg)
        seen.add(fixture.name)
        fixtures.append(fixture)

    return Manifest(version=version, master_seed=master_seed, fixtures=tuple(fixtures))


def _parse_fixture(entry: object, *, index: int, source: str) -> Fixture:
    """Validate one fixture entry into a frozen model."""
    where = f"{source}: fixture at index {index}"
    if not isinstance(entry, dict):
        msg = f"{where}: fixture must be a mapping"
        raise ManifestError(msg)

    name = entry.get("name")
    if not isinstance(name, str) or not name:
        msg = f"{where}: name must be a non-empty string"
        raise ManifestError(msg)

    # From here on every message names the fixture, not its index: "which
    # fixture is broken" is the question a reader actually has.
    where = f"{source}: fixture {name!r}"
    _reject_extra_keys(entry, _FIXTURE_KEY_ORDER, where)

    category = entry.get("category")
    if not isinstance(category, str) or not category:
        msg = f"{where}: category must be a non-empty string"
        raise ManifestError(msg)

    generator = entry.get("generator")
    if not isinstance(generator, dict):
        msg = f"{where}: generator must be a mapping"
        raise ManifestError(msg)
    kind = generator.get("kind")
    if kind not in _GENERATOR_KINDS:
        known = ", ".join(_GENERATOR_KINDS)
        msg = f"{where}: unknown generator kind {kind!r} (known: {known})"
        raise ManifestError(msg)

    expect = entry.get("expect", {})
    if not isinstance(expect, dict):
        msg = f"{where}: expect must be a mapping"
        raise ManifestError(msg)

    return Fixture(name=name, category=category, generator=generator, expect=expect)


def _reject_extra_keys(data: dict[str, Any], allowed: tuple[str, ...], where: str) -> None:
    """Reject any key not in ``allowed`` — a misspelled field must fail loudly."""
    extra = [key for key in data if key not in allowed]
    if extra:
        msg = f"{where}: unrecognized key(s) {extra!r} (known: {list(allowed)!r})"
        raise ManifestError(msg)
