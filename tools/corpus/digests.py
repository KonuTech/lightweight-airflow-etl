"""The committed digest oracle, in standard ``sha256sum`` format (D-16e).

The format is not a private convention: ``sha256sum -c
tests/fixtures/CORPUS.sha256`` must work from the repository root — an
oracle only this project's own tooling can read is an oracle only this
project can be wrong about. That is why names are written relative to the
repository root rather than as bare file names.

``generate`` rewrites this file; ``verify`` only ever reads it. That
asymmetry is what makes a generator change a reviewable diff (the digest
file moves in the same commit) instead of a silent re-baseline.

Near-verbatim port (Tier B, ~141 lines in the reference) from
``/home/user/projects/airflow-platform/tools/corpus/digests.py`` — this
module is pure stdlib with zero project-specific logic, so there was nothing
to scope down.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Final, Mapping

_READ_CHUNK: Final = 1 << 20

# GNU coreutils separates the digest from the name with two spaces in text
# mode (" *" in binary mode). This module always emits the two-space form,
# and accepts either when parsing.
_SEPARATOR: Final = "  "


class DigestFormatError(ValueError):
    """A digest listing is not in ``sha256sum`` format."""


def sha256_file(path: Path) -> str:
    """Hash a file without reading it into memory.

    Args:
        path: File to hash.

    Returns:
        The lowercase hex SHA-256 digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def format_digests(digests: Mapping[str, str]) -> str:
    """Render a digest listing in ``sha256sum`` format.

    Args:
        digests: Name to hex digest, in the order to be written — the
            manifest's declared order, so adding one fixture adds one line
            rather than reshuffling the file.

    Returns:
        The listing, newline-terminated.
    """
    return "".join(f"{digest}{_SEPARATOR}{name}\n" for name, digest in digests.items())


def parse_digests(text: str, *, source: str = "<digests>") -> dict[str, str]:
    """Parse a ``sha256sum``-format listing.

    Args:
        text: The listing.
        source: Label used in error messages.

    Returns:
        Fixture name to hex digest, in file order.

    Raises:
        DigestFormatError: If a line is not a digest followed by a name.
    """
    parsed: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        digest, separator, name = line.partition(" ")
        if not separator or not name:
            msg = f"{source}:{number}: not a sha256sum line: {line!r}"
            raise DigestFormatError(msg)
        if len(digest) != hashlib.sha256().digest_size * 2:
            msg = f"{source}:{number}: digest is not 64 hex characters: {digest!r}"
            raise DigestFormatError(msg)
        # Accept both the text-mode ("  name") and binary-mode (" *name") forms.
        parsed[name.lstrip(" *")] = digest
    return parsed


def read_digests(path: Path) -> dict[str, str]:
    """Read and parse a committed digest listing.

    Args:
        path: The oracle file.

    Returns:
        Fixture name to hex digest, in file order.

    Raises:
        DigestFormatError: If the file is missing or malformed.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"{path}: cannot read the digest oracle: {exc}"
        raise DigestFormatError(msg) from exc
    return parse_digests(text, source=str(path))


def write_digests(path: Path, digests: Mapping[str, str]) -> None:
    """Write a digest listing, replacing any previous content.

    Args:
        path: The oracle file.
        digests: Fixture name to hex digest, in declared order.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_digests(digests), encoding="utf-8")
