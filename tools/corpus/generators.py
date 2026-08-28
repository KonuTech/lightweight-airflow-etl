"""Deterministic fixture generation (D-16, GEN-01) — the R1-R10 determinism
rules, scoped to the ``tabular``, ``literal``, and ``literal_unicode`` kinds
this plan needs. ``wrapper`` (gzip/zip, R5) is intentionally unimplemented —
02-05-PLAN.md adds it when the compressed fixtures are authored. ``multipart``
has no fixture in this project's scope at all (kept only in
``manifest.GeneratorKind`` for parity, per 02-RESEARCH.md).

Ported (Tier B: read the algorithm, adapt scope; never copy verbatim) from
``/home/user/projects/airflow-platform/tools/corpus/generators.py``.

THE PSEUDO-RANDOM GENERATOR HERE IS THE CORRECT ONE. ``random`` is used, not
``secrets``: the requirement is *reproducibility*, not unpredictability. There
is no secret here — the seed is committed in the manifest. A well-meaning
"security fix" swapping in ``secrets``/``os.urandom`` would silently destroy
the byte-identity guarantee this module exists to provide.

Each rule this module honors defeats one specific, named mechanism:

* **R1** — every fixture draws from its own stream, derived from
  ``sha256(f"{master_seed}|{name}")``. A single shared stream would make
  fixture *N*'s bytes depend on how many values fixtures 1..*N*-1 consumed.
* **R2** — randomness is consumed only through ``Random.random()``. Integers
  and selections are derived by arithmetic over it — never
  ``choice``/``shuffle``/``sample``/``randrange``/``randint``, which CPython
  does not guarantee stable across versions.
* **R3** — text is built as ``str``, encoded to the declared encoding, and
  the caller (``__main__.py``) writes it in binary mode. No text-mode
  ``open()`` appears anywhere in this module.
* **R4** — the line terminator is an explicit manifest field, hand-joined.
  No writer's own default (e.g. ``csv.writer``'s ``\\r\\n``) is ever used.
* **R6** — no ``datetime.now()``, ``uuid4()``, ``os.urandom()``,
  ``time.time()``, or ``os.getpid()`` anywhere in this module.
* **R7** — the manifest (and this fixture's own header/row-spec) is iterated
  in declared order — never a ``set``, never ``sorted()`` on a heterogeneous
  key.
* **R10** — decimal values are formatted with explicit integer arithmetic
  over ``Decimal``-derived bounds, never ``str(float)``.
"""

from __future__ import annotations

import codecs
import hashlib
import random
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import TYPE_CHECKING, Any, Callable, Final

if TYPE_CHECKING:
    from .manifest import Fixture

    Renderer = Callable[[random.Random, int], str]

_BOMS: Final[dict[str, bytes]] = {
    "utf-8": codecs.BOM_UTF8,
    "utf-16": codecs.BOM_UTF16,
    "utf-16-le": codecs.BOM_UTF16_LE,
    "utf-16-be": codecs.BOM_UTF16_BE,
    "utf-32": codecs.BOM_UTF32,
    "utf-32-le": codecs.BOM_UTF32_LE,
    "utf-32-be": codecs.BOM_UTF32_BE,
}


class GeneratorError(RuntimeError):
    """A fixture could not be generated from its declaration."""


def stream_for(master_seed: str, name: str) -> random.Random:
    """Derive a fixture's private random stream (R1).

    Args:
        master_seed: The manifest's master seed.
        name: The fixture name.

    Returns:
        A generator seeded so this fixture's bytes depend on nothing but the
        master seed and its own name — never on how many other fixtures were
        generated before it.
    """
    digest = hashlib.sha256(f"{master_seed}|{name}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))  # noqa: S311 - see module docstring


def generate_fixture(fixture: Fixture, rng: random.Random) -> bytes:
    """Dispatch to the generator kind the fixture declares.

    Args:
        fixture: The validated fixture declaration.
        rng: This fixture's own private stream (from ``stream_for``).

    Returns:
        The fixture's fully-encoded bytes, ready for a binary-mode write.

    Raises:
        GeneratorError: If the fixture's declared kind is not implemented,
            or its content cannot be encoded in its declared encoding.
    """
    kind = fixture.generator.get("kind")
    if kind == "tabular":
        return _generate_tabular(fixture, rng)
    if kind == "literal":
        return _generate_literal(fixture)
    if kind == "literal_unicode":
        return _generate_literal_unicode(fixture)
    msg = (
        f"{fixture.name}: generator kind {kind!r} is not implemented in this "
        f"plan's scope (wrapper is added by 02-05-PLAN.md; multipart has no "
        f"fixture in this project)"
    )
    raise GeneratorError(msg)


def _generate_tabular(fixture: Fixture, rng: random.Random) -> bytes:
    """Write a header plus row-spec-driven rows (R2, R4, R7, R10)."""
    spec = fixture.generator
    delimiter = spec.get("delimiter", ",")
    quotechar = spec.get("quotechar", '"')
    escapechar = spec.get("escapechar")
    doublequote = spec.get("doublequote", True)
    line_terminator = spec.get("line_terminator", "\n")
    encoding = spec.get("encoding", "utf-8")
    header = tuple(spec.get("header", ()))
    rows = spec.get("rows", 0)
    row_spec = spec.get("row_spec", {})

    if not header:
        msg = f"{fixture.name}: a tabular fixture needs a non-empty header"
        raise GeneratorError(msg)

    # R7: renderers built once, in the header's own declared order.
    renderers = [_renderer_for(fixture.name, column, row_spec.get(column)) for column in header]

    def _row(fields: tuple[str, ...]) -> str:
        return delimiter.join(
            _quote_field(field, delimiter, quotechar, escapechar, doublequote) for field in fields
        )

    lines = [_row(header)]
    for row_index in range(rows):
        lines.append(_row(tuple(render(rng, row_index) for render in renderers)))

    # R4: the terminator is the manifest's own explicit field, hand-joined —
    # never csv.writer's own default, never a translated "\n".
    text = line_terminator.join(lines) + line_terminator
    return _encode(fixture.name, text, encoding)


def _generate_literal(fixture: Fixture) -> bytes:
    """Write raw declared content, encoded per the fixture's declared encoding.

    R3 is the caller's responsibility (``__main__.py`` writes with
    ``open(path, "wb")``); this function only ever returns ``bytes``.
    """
    spec = fixture.generator
    content = spec.get("content")
    if content is None:
        msg = f"{fixture.name}: literal fixture has no content"
        raise GeneratorError(msg)

    encoding = spec.get("encoding", "utf-8")
    encoded = _encode(fixture.name, content, encoding)
    if spec.get("bom", False):
        encoded = _bom_for(fixture.name, encoding) + encoded
    return encoded


def _generate_literal_unicode(fixture: Fixture) -> bytes:
    """Same mechanism as ``literal`` — kept a distinct kind for parity with
    the reference repo/D-16's vocabulary. Its content may declare a BOM
    prefix (``bom: true``) or rely on a codec (e.g. ``utf-16``) that already
    embeds one.
    """
    return _generate_literal(fixture)


def _renderer_for(fixture_name: str, column: str, spec: dict[str, Any] | None) -> Renderer:
    """Build the per-row renderer for one row-spec column."""
    if not isinstance(spec, dict):
        msg = f"{fixture_name}: column {column!r} has no row_spec entry"
        raise GeneratorError(msg)
    kind = spec.get("kind")
    if kind == "zero_padded_int":
        return _zero_padded_renderer(spec)
    if kind == "pick":
        return _pick_renderer(spec)
    if kind == "decimal":
        return _decimal_renderer(spec)
    if kind == "repeat":
        return _repeat_renderer(spec)
    msg = f"{fixture_name}: column {column!r} has unknown row_spec kind {kind!r}"
    raise GeneratorError(msg)


def _zero_padded_renderer(spec: dict[str, Any]) -> Renderer:
    """Render a monotonic integer with leading zeros — consumes no randomness."""
    width = spec["width"]
    start = spec["start"]

    def _render(rng: random.Random, row_index: int) -> str:
        del rng
        return f"{start + row_index:0{width}d}"

    return _render


def _pick_renderer(spec: dict[str, Any]) -> Renderer:
    """Select from a fixed list by index arithmetic over ``.random()`` (R2).

    Never ``rng.choice()`` — only its own documented-stable ``.random()``
    sequence.
    """
    values = tuple(spec["values"])
    count = len(values)

    def _render(rng: random.Random, row_index: int) -> str:
        del row_index
        return values[min(int(rng.random() * count), count - 1)]

    return _render


def _decimal_renderer(spec: dict[str, Any]) -> Renderer:
    """Render an exact decimal via integer arithmetic — never a float (R10)."""
    scale = spec["scale"]
    separator = spec.get("decimal_separator", ".")
    minimum = Decimal(str(spec["min"]))
    maximum = Decimal(str(spec["max"]))
    power = 10**scale
    low = int(minimum.scaleb(scale).to_integral_value(rounding=ROUND_CEILING))
    high = int(maximum.scaleb(scale).to_integral_value(rounding=ROUND_FLOOR))
    span = high - low + 1

    if scale == 0:

        def _render_integral(rng: random.Random, row_index: int) -> str:
            del row_index
            return str(low + min(int(rng.random() * span), span - 1))

        return _render_integral

    def _render(rng: random.Random, row_index: int) -> str:
        del row_index
        units = low + min(int(rng.random() * span), span - 1)
        return f"{units // power}{separator}{units % power:0{scale}d}"

    return _render


def _repeat_renderer(spec: dict[str, Any]) -> Renderer:
    """Render a field of an exact declared width — consumes no randomness."""
    value = spec["char"] * spec["length"]

    def _render(rng: random.Random, row_index: int) -> str:
        del rng, row_index
        return value

    return _render


def _quote_field(
    value: str,
    delimiter: str,
    quotechar: str,
    escapechar: str | None,
    doublequote: bool,
) -> str:
    """Quote a field only if it needs it, using the fixture's own dialect.

    A field needs quoting if it contains the delimiter, the quote character,
    a line terminator, or (when declared) the escape character. Embedded
    quote characters are escaped by doubling (``doublequote: true``) or by a
    declared ``escapechar`` prefix (``doublequote: false``) — exercising
    D-02's explicit escapechar/doublequote test-coverage request.
    """
    needs_quoting = (
        delimiter in value
        or quotechar in value
        or "\n" in value
        or "\r" in value
        or (escapechar is not None and escapechar in value)
    )
    if not needs_quoting:
        return value

    if doublequote:
        escaped = value.replace(quotechar, quotechar * 2)
    else:
        if not escapechar:
            msg = (
                f"field {value!r} requires escaping but doublequote is false "
                f"and no escapechar is declared"
            )
            raise GeneratorError(msg)
        # Escape the escape character itself first, then the quote character,
        # so an escapechar that also appears literally in the value is not
        # double-counted.
        escaped = value.replace(escapechar, escapechar * 2)
        escaped = escaped.replace(quotechar, f"{escapechar}{quotechar}")
    return f"{quotechar}{escaped}{quotechar}"


def _encode(fixture_name: str, text: str, encoding: str) -> bytes:
    """Encode with a strict error policy, naming the fixture if it cannot."""
    try:
        return text.encode(encoding, "strict")
    except UnicodeEncodeError as exc:
        msg = (
            f"{fixture_name}: encoding {encoding!r} cannot represent "
            f"{exc.object[exc.start : exc.end]!r}"
        )
        raise GeneratorError(msg) from exc


def _bom_for(fixture_name: str, encoding: str) -> bytes:
    """Return the byte-order mark for an encoding that has one."""
    name = codecs.lookup(encoding).name
    bom = _BOMS.get(name)
    if bom is None:
        msg = f"{fixture_name}: encoding {encoding!r} has no known byte-order mark"
        raise GeneratorError(msg)
    return bom
