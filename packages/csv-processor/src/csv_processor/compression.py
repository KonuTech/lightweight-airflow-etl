"""Magic-byte compression detection + streaming gzip/zip open (D-29/D-30/D-33).

``detect_compression`` is NEW code, not a Tier-A port -- the reference repo's
own ``compression.py`` dispatches by filename extension
(``_EXTENSION_COMPRESSION = {".gz": "gzip", ".zip": "zip"}``), which
contradicts this project's own locked D-30 (magic-byte sniffing,
pattern-agnostic). See 03-RESEARCH.md Pitfall 1 and 03-PATTERNS.md's "D-30
vs. compression.py's actual approach" note -- do not cite the reference file
as prior art for this function.

``open_compressed_stream``'s streaming-open mechanics ARE ported from the
reference repo (S3-backed ``dataplat.storage.objectstore.open_text_stream``
swapped for a plain local ``Path.open("rb")``): ``.gz`` decompresses in a
true single-pass stream via ``gzip.GzipFile(fileobj=io.BufferedReader(...))``
(D-29's "never extract-to-a-temp-file" requirement); ``.zip`` cannot do this
-- ``zipfile.ZipFile`` needs its central directory (which lives at the
archive's end) before it can open any member, a structural property of the
ZIP format itself -- so the *compressed* archive bytes (never the
decompressed CSV content, never disk) are buffered into ``io.BytesIO``
first, bounded by D-33's "exactly one member" rule.

``_DecompressionBombGuard`` (ported verbatim apart from the exception type
and its context key) enforces T-03-09's cumulative-decompressed-byte
ceiling via bounded incremental reads -- never a single unbounded ``.read()``
call to the underlying decompressor, which would defeat the entire point of
streaming.
"""

from __future__ import annotations

import gzip
import io
import zipfile
from typing import TYPE_CHECKING, BinaryIO, Final

from csv_processor.errors import (
    CORRUPTED_ARCHIVE,
    DECOMPRESSION_BOMB_EXCEEDED,
    FileInspectionError,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Protocol

    class _ReadCloseable(Protocol):
        """The minimal surface ``_DecompressionBombGuard`` needs from what it wraps."""

        def read(self, size: int = ...) -> bytes: ...
        def close(self) -> None: ...


# Every underlying decompressor .read() call is capped to this many bytes,
# regardless of what a caller (including io.TextIOWrapper's own "read
# everything" path) requests -- this is what makes the decompression-bomb
# ceiling enforceable incrementally: a single call can never materialize
# more than this many decompressed bytes before the cumulative check in
# _DecompressionBombGuard runs again.
_BOUNDED_READ_CHUNK_BYTES: Final[int] = 65_536  # 64 KiB

# A generous but bounded default -- comfortably above any real fixture this
# project generates, far below what an actual decompression-bomb attack
# would need to be dangerous. Overridable per call.
_DEFAULT_MAX_DECOMPRESSED_BYTES: Final[int] = 512 * 1024 * 1024  # 512 MiB


def detect_compression(sample: bytes) -> str | None:
    """Sniff a compression kind from magic bytes only (D-30) -- new code, not a port.

    Args:
        sample: The first few bytes of a file (any length >= 4 is enough;
            shorter samples simply never match either magic-byte prefix).

    Returns:
        ``"gzip"`` if ``sample`` starts with the gzip magic bytes
        (``0x1f 0x8b``), ``"zip"`` if it starts with the zip local-file-header
        signature (``PK\\x03\\x04``), else ``None`` -- never inspects a
        filename or extension.
    """
    if sample[:2] == b"\x1f\x8b":
        return "gzip"
    if sample[:4] == b"PK\x03\x04":
        return "zip"
    return None


class _DecompressionBombGuard:
    """Wraps a decompressed binary stream, tripping once cumulative bytes exceed a ceiling.

    Every underlying read is capped at ``_BOUNDED_READ_CHUNK_BYTES``
    (T-03-09) regardless of how many bytes the caller requests -- this is
    what makes the ceiling enforceable incrementally: a single call, even
    ``io.TextIOWrapper``'s own "read everything" path, can never materialize
    more than one bounded chunk of decompressed content before the
    cumulative check below runs again.

    Duck-types enough of a binary stream's surface (``readable``/
    ``writable``/``seekable``/``closed``/``read``/``read1``/``close``) for
    ``io.TextIOWrapper`` to accept it directly -- ported verbatim from the
    reference repo's own equivalent class, apart from the exception type
    (``FileInspectionError`` instead of ``dataplat.errors.FileInspectionError``)
    and its context dict's diagnostic key (``error_code`` instead of
    ``diagnostic_code``, matching this project's own vocabulary convention).
    """

    def __init__(self, inner: _ReadCloseable, *, max_decompressed_bytes: int) -> None:
        """Wrap ``inner`` (a ``gzip.GzipFile`` or ``zipfile.ZipExtFile``) with the ceiling check.

        Args:
            inner: The real decompressor to read bounded chunks from.
            max_decompressed_bytes: The cumulative decompressed-byte ceiling.
        """
        self._inner = inner
        self._max_decompressed_bytes = max_decompressed_bytes
        self._bytes_read = 0
        self.closed = False

    def readable(self) -> bool:
        """Report this stream as readable, as ``io.TextIOWrapper`` requires."""
        return True

    def writable(self) -> bool:
        """Report this stream as not writable, as ``io.TextIOWrapper`` requires."""
        return False

    def seekable(self) -> bool:
        """Report this stream as not seekable -- reading is genuinely one-directional."""
        return False

    def flush(self) -> None:
        """No-op: a read-only stream never has anything buffered to flush.

        ``io.TextIOWrapper.close()`` unconditionally calls ``self.buffer.flush()``
        before closing the buffer it wraps -- both the gzip and zip paths hand
        this guard to ``io.TextIOWrapper`` directly as its ``buffer`` (no
        intermediate ``io.BufferedReader``), so without this method a
        fully-successful read would raise ``AttributeError`` the moment a
        caller closed the stream.
        """
        return

    def _read_one_bounded_chunk(self, requested: int | None) -> bytes:
        """Perform exactly one bounded call to the real decompressor, checking the ceiling.

        Args:
            requested: The caller's requested size, or ``None``/negative for
                "as much as available" -- either way, the actual call to
                ``inner.read()`` is capped at ``_BOUNDED_READ_CHUNK_BYTES``.

        Returns:
            The chunk read, which may be empty at genuine EOF.

        Raises:
            FileInspectionError: Cumulative bytes read across this guard's
                lifetime now exceed ``max_decompressed_bytes``
                (``error_code=DECOMPRESSION_BOMB_EXCEEDED``).
        """
        want = (
            _BOUNDED_READ_CHUNK_BYTES
            if requested is None or requested < 0
            else min(requested, _BOUNDED_READ_CHUNK_BYTES)
        )
        chunk = self._inner.read(want)
        self._bytes_read += len(chunk)
        if self._bytes_read > self._max_decompressed_bytes:
            msg = (
                f"decompressed content exceeds the configured "
                f"{self._max_decompressed_bytes}-byte ceiling"
            )
            raise FileInspectionError(
                msg,
                context={
                    "error_code": DECOMPRESSION_BOMB_EXCEEDED,
                    "max_decompressed_bytes": self._max_decompressed_bytes,
                    "bytes_read_before_trip": self._bytes_read,
                },
            )
        return chunk

    def read1(self, size: int = -1) -> bytes:
        """A single bounded read; short reads are allowed (``io.TextIOWrapper``'s fast path).

        Args:
            size: The caller's requested size, or ``-1`` for "as much as
                available in one call".

        Returns:
            Up to ``size`` bytes (never more than
            ``_BOUNDED_READ_CHUNK_BYTES``), possibly fewer even before EOF --
            that short-read behavior is exactly what ``read1`` promises.
        """
        return self._read_one_bounded_chunk(size)

    def read(self, size: int = -1) -> bytes:
        """The full ``read(size)`` contract: loop bounded reads until ``size``/EOF.

        Never delegates a caller's ``size=-1`` ("read everything") straight
        to the real decompressor -- that single call could materialize an
        entire decompression-bomb payload before this guard ever gets a
        chance to check it (T-03-09). Looping bounded reads instead means
        the cumulative ceiling is checked after every
        ``_BOUNDED_READ_CHUNK_BYTES``, never after consuming the whole
        stream.

        Args:
            size: The number of bytes requested, or ``-1``/negative to read
                until EOF.

        Returns:
            Exactly ``size`` bytes (or fewer only at genuine EOF) when
            ``size`` is non-negative; the entire remaining stream when
            ``size`` is negative.
        """
        pieces: list[bytes] = []
        if size is not None and size >= 0:
            remaining = size
            while remaining > 0:
                chunk = self._read_one_bounded_chunk(remaining)
                if not chunk:
                    break
                pieces.append(chunk)
                remaining -= len(chunk)
            return b"".join(pieces)
        while True:
            chunk = self._read_one_bounded_chunk(None)
            if not chunk:
                break
            pieces.append(chunk)
        return b"".join(pieces)

    def close(self) -> None:
        """Close the wrapped decompressor and mark this guard closed."""
        self._inner.close()
        self.closed = True


def open_compressed_stream(
    file_path: Path,
    *,
    compression: str | None,
    max_decompressed_bytes: int = _DEFAULT_MAX_DECOMPRESSED_BYTES,
) -> BinaryIO:
    """Open ``file_path``'s bytes as a binary stream, decompressing if declared.

    Args:
        file_path: The file to open.
        compression: ``None`` for a plain passthrough open (an uncompressed
            file's on-disk size already bounds its content -- no
            expansion-ratio attack surface, no guard needed), ``"gzip"`` for
            a true single-pass ``.gz`` stream, or ``"zip"`` for the
            buffered-archive-bytes exception D-33 requires.
        max_decompressed_bytes: The decompression-bomb ceiling (T-03-09).
            Ignored when ``compression`` is ``None``.

    Returns:
        A binary stream over the (possibly decompressed) content -- callers
        (``source.py``) wrap it in ``io.TextIOWrapper`` themselves.

    Raises:
        FileInspectionError: ``compression == "zip"`` and the archive is
            corrupted/truncated, or contains anything other than exactly one
            member (both ``error_code=CORRUPTED_ARCHIVE``, D-33); or either
            path's cumulative decompressed bytes exceed
            ``max_decompressed_bytes`` (``error_code=DECOMPRESSION_BOMB_EXCEEDED``).
    """
    if compression is None:
        return file_path.open("rb")

    if compression == "gzip":
        buffered = io.BufferedReader(file_path.open("rb"))
        decompressed = gzip.GzipFile(fileobj=buffered, mode="rb")
        return _DecompressionBombGuard(  # type: ignore[return-value]
            decompressed, max_decompressed_bytes=max_decompressed_bytes
        )

    if compression == "zip":
        return _open_zip_stream(file_path, max_decompressed_bytes=max_decompressed_bytes)

    msg = f"unsupported compression {compression!r}"
    raise FileInspectionError(msg, context={"error_code": CORRUPTED_ARCHIVE})


def _open_zip_stream(file_path: Path, *, max_decompressed_bytes: int) -> BinaryIO:
    """Buffer a ``.zip`` archive's compressed bytes, then stream its one member (D-33).

    Args:
        file_path: The archive to open. Its *compressed* bytes are read
            fully into memory (``Path.read_bytes()``) -- bounded by the
            archive's compressed size (D-33 scopes every archive to exactly
            one CSV member), never the decompressed CSV content, never disk.
            ``zipfile.ZipFile`` structurally requires random access to the
            archive's end-of-file member index before it can open anything,
            so this buffering is not a shortcut around streaming -- it is
            the only way to open a zip member at all.
        max_decompressed_bytes: The decompression-bomb ceiling (T-03-09).

    Returns:
        A binary stream over the archive's single member's decompressed
        content, guarded against a decompression bomb.

    Raises:
        FileInspectionError: The archive (or its one member) is corrupted or
            truncated, or the archive holds anything other than exactly one
            member -- both ``error_code=CORRUPTED_ARCHIVE`` (D-33).
    """
    compressed_bytes = file_path.read_bytes()

    try:
        archive = zipfile.ZipFile(io.BytesIO(compressed_bytes))
    except zipfile.BadZipFile as exc:
        msg = "corrupted or truncated zip archive"
        raise FileInspectionError(msg, context={"error_code": CORRUPTED_ARCHIVE}) from exc

    with archive:
        names = archive.namelist()
        if len(names) != 1:
            msg = f"zip archive must contain exactly one member (D-33), found {len(names)}"
            raise FileInspectionError(
                msg,
                context={"error_code": CORRUPTED_ARCHIVE, "member_count": len(names)},
            )
        try:
            # archive.open() returns a zipfile.ZipExtFile. Closing `archive`
            # below (this `with` block's own exit) does not invalidate it --
            # ZipExtFile reads directly from the underlying io.BytesIO, which
            # stays alive via this closure, independent of the parent
            # ZipFile object's own lifecycle.
            member = archive.open(names[0])
        except zipfile.BadZipFile as exc:
            msg = f"zip archive member {names[0]!r} is corrupted"
            raise FileInspectionError(msg, context={"error_code": CORRUPTED_ARCHIVE}) from exc

    return _DecompressionBombGuard(  # type: ignore[return-value]
        member, max_decompressed_bytes=max_decompressed_bytes
    )
