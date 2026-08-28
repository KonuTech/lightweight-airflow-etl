"""The deterministic CSV fixture-corpus generator (D-16, GEN-01).

See ``tools/corpus/manifest.py`` for the manifest schema, ``generators.py``
for the byte-construction mechanism (the ten determinism rules, R1-R10), and
``digests.py`` for the committed ``sha256sum``-compatible oracle format.
Run via ``python -m tools.corpus generate|verify`` (``__main__.py``), wired
to ``make fixtures``/``make fixtures-verify``.
"""

from __future__ import annotations

from .manifest import Fixture, Manifest, ManifestError, load_manifest, load_manifest_with_seed

__all__ = [
    "Fixture",
    "Manifest",
    "ManifestError",
    "load_manifest",
    "load_manifest_with_seed",
]
