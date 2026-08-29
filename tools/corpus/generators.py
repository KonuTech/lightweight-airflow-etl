"""Deterministic fixture generation (D-16, GEN-01) — the R1-R10 determinism
rules, covering the ``tabular``, ``literal``, ``literal_unicode``, and
``wrapper`` kinds. ``wrapper`` (gzip/zip, R5) was added by 02-05-PLAN.md for
the large/compressed fixture category. ``multipart`` has no fixture in this
project's scope at all (kept only in ``manifest.GeneratorKind`` for parity,
per 02-RESEARCH.md).

Unlike the reference repo's ``wrapper`` kind (which compresses an
already-materialised sibling fixture file read back off disk), this
project's compressed fixtures are small and self-contained: the inner
payload is declared inline (``generator.inner``) and generated in-memory
from the *same* private RNG stream as the wrapper fixture itself (R1 — the
wrapped fixture's bytes still depend on nothing but its own name and the
master seed, never on a second fixture's generation order).

The ``profile: large`` tabular variant (``_generate_tabular_batched``) exists
purely to keep 02-05-PLAN.md's ~60 MiB fixture from ever holding every
rendered row simultaneously in one Python list before joining — it produces
byte-identical output to the plain ``_generate_tabular`` path for the same
declaration; batching is a memory-shape difference, never a format one.

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
import gzip
import hashlib
import io
import random
import zipfile
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import TYPE_CHECKING, Any, Callable, Final

from .manifest import Fixture

if TYPE_CHECKING:
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


# `profile: large` batching (see module docstring) — rows are encoded and
# flushed into the output buffer in chunks of this size rather than all at
# once, so the largest transient object is one batch's worth of `str`s, not
# the whole file's.
_LARGE_BATCH_ROWS: Final[int] = 5000

# R5: both non-negotiable. gzip embeds the current wall-clock time and the
# source file name in its header, so without pinning both, two runs a second
# apart produce different bytes.
_GZIP_COMPRESSLEVEL: Final[int] = 9
_GZIP_MTIME: Final[int] = 0
_GZIP_FILENAME: Final[str] = ""

# R5's zip counterpart: `ZipInfo.date_time` defaults to the current wall-clock
# time, which would make this fixture exactly as non-reproducible across runs
# as an unpinned gzip mtime. 1980-01-01 is ZIP's minimum representable date,
# pinned explicitly rather than left to the default.
_ZIP_DATE_TIME: Final[tuple[int, int, int, int, int, int]] = (1980, 1, 1, 0, 0, 0)
_ZIP_DEFAULT_INNER_FILENAME: Final[str] = "payload.csv"


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
        if fixture.generator.get("profile") == "large":
            return _generate_tabular_batched(fixture, rng)
        return _generate_tabular(fixture, rng)
    if kind == "literal":
        return _generate_literal(fixture)
    if kind == "literal_unicode":
        return _generate_literal_unicode(fixture)
    if kind == "wrapper":
        return _generate_wrapper(fixture, rng)
    msg = (
        f"{fixture.name}: generator kind {kind!r} is not implemented in this "
        f"project's scope (multipart has no fixture in this project)"
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


def _generate_tabular_batched(fixture: Fixture, rng: random.Random) -> bytes:
    """Batched variant of ``_generate_tabular`` for ``profile: large`` fixtures.

    Produces byte-identical output to ``_generate_tabular`` for the same
    declaration (proven by ``test_corpus_generators.py``'s byte-for-byte
    equivalence test) but never holds more than ``_LARGE_BATCH_ROWS`` rendered
    rows in memory at once — rows are encoded and flushed into a growing
    ``bytearray`` in batches instead of collected into one Python list and
    joined in a single pass at the end.
    """
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
    if encoding in {"utf-16", "utf-32"}:
        # Bare utf-16/utf-32 codecs prepend a byte-order mark on every
        # encode() call -- batching per-chunk would embed one BOM per batch
        # instead of one for the whole file. No large/wrapper fixture in this
        # project's scope declares a bare multi-byte encoding; this is a
        # defensive guard against a future combination that would silently
        # corrupt the file, not a supported one.
        msg = (
            f"{fixture.name}: profile 'large' does not support bare {encoding!r} "
            f"(would embed a byte-order mark per batch, not once for the file)"
        )
        raise GeneratorError(msg)

    # R7: renderers built once, in the header's own declared order.
    renderers = [_renderer_for(fixture.name, column, row_spec.get(column)) for column in header]

    def _row(fields: tuple[str, ...]) -> str:
        return delimiter.join(
            _quote_field(field, delimiter, quotechar, escapechar, doublequote) for field in fields
        )

    term_bytes = _encode(fixture.name, line_terminator, encoding)
    buffer = bytearray()
    buffer += _encode(fixture.name, _row(header), encoding)
    buffer += term_bytes

    batch: list[str] = []
    for row_index in range(rows):
        batch.append(_row(tuple(render(rng, row_index) for render in renderers)))
        if len(batch) >= _LARGE_BATCH_ROWS:
            buffer += _encode(fixture.name, line_terminator.join(batch), encoding)
            buffer += term_bytes
            batch = []
    if batch:
        buffer += _encode(fixture.name, line_terminator.join(batch), encoding)
        buffer += term_bytes

    return bytes(buffer)


def _generate_wrapper(fixture: Fixture, rng: random.Random) -> bytes:
    """Compress an inline inner payload with deterministic wrapper headers (R5).

    ``generator.inner`` declares a small, self-contained payload spec (any
    already-implemented kind, typically ``tabular``); it is generated first
    via the normal dispatcher, reusing this fixture's own private RNG stream
    (R1), then wrapped per ``generator.format``.
    """
    spec = fixture.generator
    inner_spec = spec.get("inner")
    if not isinstance(inner_spec, dict):
        msg = f"{fixture.name}: wrapper fixture needs an 'inner' generator mapping"
        raise GeneratorError(msg)

    inner_fixture = Fixture(
        name=fixture.name, category=fixture.category, generator=inner_spec, expect=fixture.expect
    )
    raw = generate_fixture(inner_fixture, rng)

    fmt = spec.get("format")
    if fmt == "gzip":
        return _wrap_gzip(raw)
    if fmt == "zip":
        member_name = spec.get("inner_filename", _ZIP_DEFAULT_INNER_FILENAME)
        return _wrap_zip(member_name, raw)
    msg = f"{fixture.name}: wrapper format {fmt!r} is not supported (want 'gzip' or 'zip')"
    raise GeneratorError(msg)


def _wrap_gzip(raw: bytes) -> bytes:
    """Gzip-wrap bytes with deterministic headers (R5): ``mtime=0``, ``filename=""``."""
    buffer = io.BytesIO()
    with gzip.GzipFile(
        fileobj=buffer,
        mode="wb",
        compresslevel=_GZIP_COMPRESSLEVEL,
        mtime=_GZIP_MTIME,
        filename=_GZIP_FILENAME,
    ) as compressed:
        compressed.write(raw)
    return buffer.getvalue()


def _wrap_zip(member_name: str, raw: bytes) -> bytes:
    """Zip-wrap bytes with a deterministic member timestamp (R5): 1980-01-01."""
    member = zipfile.ZipInfo(filename=member_name, date_time=_ZIP_DATE_TIME)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr(member, raw)
    return buffer.getvalue()


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
        return _zero_padded_renderer(fixture_name, column, spec)
    if kind == "pick":
        return _pick_renderer(fixture_name, column, spec)
    if kind == "decimal":
        return _decimal_renderer(fixture_name, column, spec)
    if kind == "repeat":
        return _repeat_renderer(fixture_name, column, spec)
    msg = f"{fixture_name}: column {column!r} has unknown row_spec kind {kind!r}"
    raise GeneratorError(msg)


def _zero_padded_renderer(fixture_name: str, column: str, spec: dict[str, Any]) -> Renderer:
    """Render a monotonic integer with leading zeros — consumes no randomness."""
    try:
        width = spec["width"]
        start = spec["start"]
    except KeyError as exc:
        msg = f"{fixture_name}: column {column!r} row_spec missing {exc.args[0]!r}"
        raise GeneratorError(msg) from exc

    def _render(rng: random.Random, row_index: int) -> str:
        del rng
        return f"{start + row_index:0{width}d}"

    return _render


def _pick_renderer(fixture_name: str, column: str, spec: dict[str, Any]) -> Renderer:
    """Select from a fixed list by index arithmetic over ``.random()`` (R2).

    Never ``rng.choice()`` — only its own documented-stable ``.random()``
    sequence.
    """
    try:
        values = tuple(spec["values"])
    except KeyError as exc:
        msg = f"{fixture_name}: column {column!r} row_spec missing {exc.args[0]!r}"
        raise GeneratorError(msg) from exc
    count = len(values)

    def _render(rng: random.Random, row_index: int) -> str:
        del row_index
        return values[min(int(rng.random() * count), count - 1)]

    return _render


def _decimal_renderer(fixture_name: str, column: str, spec: dict[str, Any]) -> Renderer:
    """Render an exact decimal via integer arithmetic — never a float (R10)."""
    try:
        scale = spec["scale"]
        minimum = Decimal(str(spec["min"]))
        maximum = Decimal(str(spec["max"]))
    except KeyError as exc:
        msg = f"{fixture_name}: column {column!r} row_spec missing {exc.args[0]!r}"
        raise GeneratorError(msg) from exc
    separator = spec.get("decimal_separator", ".")
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


def _repeat_renderer(fixture_name: str, column: str, spec: dict[str, Any]) -> Renderer:
    """Render a field of an exact declared width — consumes no randomness."""
    try:
        value = spec["char"] * spec["length"]
    except KeyError as exc:
        msg = f"{fixture_name}: column {column!r} row_spec missing {exc.args[0]!r}"
        raise GeneratorError(msg) from exc

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
