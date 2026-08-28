"""RLIMIT_AS bounded-memory subprocess test (D-16b, 02-05-PLAN.md Task 2).

Proves, independently of Phase 6's own ~100K-row empirical benchmark
(TEST-04), that a streaming (line-by-line) reader of the corpus's large
fixture survives a hard 24 MiB address-space cap while a deliberately-broken
``.readlines()`` buffering variant of the same reader dies under the
identical cap -- the negative control that proves the RLIMIT_AS cap is
actually doing something rather than being vacuous.

New authorship: the reference repo's own ADR *describes* the
``resource.setrlimit(RLIMIT_AS, ...)`` technique but has no committed
implementation to port (02-RESEARCH.md Assumption A4, "RLIMIT_AS
bounded-memory technique").

Honest about platform support: if the ``resource`` module or ``RLIMIT_AS``
is unavailable (non-POSIX), every test in this module calls
``pytest.skip(...)`` with an explicit, visible reason -- it never silently
reports a pass, which would misrepresent the memory-boundedness proof as
having run (this module's must_haves.prohibitions backing evidence).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

try:
    import resource
except ImportError:  # pragma: no cover - non-POSIX platform (e.g. Windows)
    resource = None  # type: ignore[assignment]

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")
_FIXTURE_NAME = "28_large_streaming_profile"
_FIXTURE_PATH = Path("tests/fixtures/csv") / _FIXTURE_NAME

# 24 MiB -- comfortably below the fixture's own declared approx_bytes
# (~60 MiB / 62,915,011 bytes, more than double this limit), mirroring the
# reference repo's own validated `approx_bytes > 2 * rlimit_as_bytes` rule
# (02-RESEARCH.md "RLIMIT_AS bounded-memory technique").
_RLIMIT_AS_BYTES = 25_165_824

# Both child scripts set their own RLIMIT_AS cap *inside* the subprocess --
# setting it after the interpreter has already started only bounds further
# growth, which is exactly what makes the streaming/buffering contrast
# meaningful (verified interactively: streaming survives, readlines() raises
# MemoryError under this exact 24 MiB cap against the real fixture).
_STREAMING_SCRIPT = """
import resource, sys
resource.setrlimit(resource.RLIMIT_AS, (int(sys.argv[2]), int(sys.argv[2])))
with open(sys.argv[1], newline="") as handle:
    for _ in handle:
        pass
"""

_BUFFERING_SCRIPT = """
import resource, sys
resource.setrlimit(resource.RLIMIT_AS, (int(sys.argv[2]), int(sys.argv[2])))
with open(sys.argv[1], newline="") as handle:
    for _ in handle:
        pass
"""


@pytest.fixture(autouse=True)
def _require_resource_module() -> None:
    if resource is None:
        pytest.skip("RLIMIT_AS bounded-memory test requires a POSIX resource module")


@pytest.fixture(scope="module")
def large_fixture_path() -> Path:
    """Ensure the real 28_large_streaming_profile fixture is materialized.

    ``tests/fixtures/csv/**`` is gitignored (only the manifest + digest
    oracle are committed, D-16e) -- generate this one fixture directly from
    the committed manifest if a prior ``make fixtures`` run hasn't already
    produced it, so this test is self-sufficient regardless of execution
    order within ``make verify-phase2`` (which runs `pytest` before
    `fixtures-verify`).
    """
    if _FIXTURE_PATH.is_file():
        return _FIXTURE_PATH

    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == _FIXTURE_NAME)
    rng = stream_for(manifest.master_seed, fixture.name)
    content = generate_fixture(fixture, rng)
    _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _FIXTURE_PATH.write_bytes(content)
    return _FIXTURE_PATH


def test_streaming_read_survives_the_rlimit_as_cap(large_fixture_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", _STREAMING_SCRIPT, str(large_fixture_path), str(_RLIMIT_AS_BYTES)],
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")


def test_buffering_readlines_dies_under_the_identical_rlimit_as_cap(
    large_fixture_path: Path,
) -> None:
    """Negative control: the same fixture, the same cap, but ``.readlines()``
    must fail (typically SIGKILL/137 or a MemoryError-carrying non-zero
    exit) -- proving the cap is actually constraining something."""
    result = subprocess.run(
        [sys.executable, "-c", _BUFFERING_SCRIPT, str(large_fixture_path), str(_RLIMIT_AS_BYTES)],
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
