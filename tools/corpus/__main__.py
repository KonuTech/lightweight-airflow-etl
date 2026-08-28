"""CLI: ``python -m tools.corpus generate|verify`` (D-16f).

``generate`` materialises the corpus into a directory and (optionally)
rewrites the digest oracle. ``verify`` regenerates into a temporary directory
and compares against the committed oracle without ever reading the on-disk
corpus and without touching the oracle itself — its only two inputs are the
manifest and the committed digest file, which is exactly why a generator
change shows up as a reviewable diff instead of silently re-baselining every
downstream expectation.

Ported (Tier B: read the algorithm, adapt scope) from
``/home/user/projects/airflow-platform/tools/corpus/__main__.py``, scoped
down: no ``--fast``/large-profile skip flag, since this plan's fixtures carry
no ``profile: large`` fixture yet (02-05-PLAN.md adds the large/compressed
category).
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from .digests import read_digests, sha256_file, write_digests
from .generators import generate_fixture, stream_for
from .manifest import Manifest, load_manifest_with_seed

_DEFAULT_MANIFEST = Path("tests/fixtures/corpus.yaml")
_DEFAULT_OUT = Path("tests/fixtures/csv")
_DEFAULT_DIGESTS = Path("tests/fixtures/CORPUS.sha256")

# Names in the oracle are relative to the repository root so `sha256sum -c`
# works there, independent of `--out` (verify regenerates into a throwaway
# directory whose path must never reach the oracle).
_DIGEST_PREFIX = "tests/fixtures/csv"


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser.

    Returns:
        The configured parser for ``generate`` and ``verify``.
    """
    parser = argparse.ArgumentParser(
        prog="python -m tools.corpus",
        description="Generate and verify the deterministic CSV fixture corpus.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="materialise the corpus")
    generate.add_argument("--manifest", type=Path, default=_DEFAULT_MANIFEST)
    generate.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    generate.add_argument(
        "--write-digests",
        type=Path,
        default=None,
        metavar="PATH",
        help="rewrite the digest oracle at PATH",
    )

    verify = subparsers.add_parser("verify", help="prove byte-identity against the oracle")
    verify.add_argument("--manifest", type=Path, default=_DEFAULT_MANIFEST)
    verify.add_argument("--digests", type=Path, default=_DEFAULT_DIGESTS)
    return parser


def _generate_corpus(manifest: Manifest, out_dir: Path) -> dict[str, str]:
    """Materialise every declared fixture and return its digest.

    Args:
        manifest: The validated corpus specification.
        out_dir: Directory the fixtures are written into. Created if absent.

    Returns:
        Fixture name (relative to ``out_dir``) to hex SHA-256 digest, in
        declared order (R7).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    digests: dict[str, str] = {}
    for fixture in manifest.fixtures:  # R7: declared order, never sorted
        rng = stream_for(manifest.master_seed, fixture.name)
        content = generate_fixture(fixture, rng)
        path = out_dir / fixture.name
        # R3: binary mode, always. A text-mode open would consult
        # locale.getpreferredencoding(False), which differs machine to machine.
        with path.open("wb") as handle:
            handle.write(content)
        digests[fixture.name] = sha256_file(path)
    return digests


def _qualify(digests: dict[str, str], prefix: str) -> dict[str, str]:
    """Prefix bare fixture names with the corpus directory."""
    if not prefix:
        return dict(digests)
    root = prefix.rstrip("/")
    return {f"{root}/{name}": digest for name, digest in digests.items()}


def command_generate(args: argparse.Namespace) -> int:
    """Materialise the corpus and optionally rewrite the oracle.

    Args:
        args: Parsed arguments.

    Returns:
        Process exit status.
    """
    manifest = load_manifest_with_seed(args.manifest)
    digests = _generate_corpus(manifest, args.out)

    if args.write_digests is not None:
        write_digests(args.write_digests, _qualify(digests, _DIGEST_PREFIX))
        print(f"wrote {len(digests)} digests to {args.write_digests}")
    else:
        print(f"generated {len(digests)} fixtures into {args.out}")
    return 0


def command_verify(args: argparse.Namespace) -> int:
    """Regenerate into a temporary directory and compare against the oracle.

    Args:
        args: Parsed arguments.

    Returns:
        Process exit status: 0 when every regenerated fixture matches.
    """
    manifest = load_manifest_with_seed(args.manifest)
    expected = read_digests(args.digests)

    with tempfile.TemporaryDirectory(prefix="corpus-verify-") as tmp:
        actual = _qualify(_generate_corpus(manifest, Path(tmp)), _DIGEST_PREFIX)

    problems: list[str] = []
    for name, digest in actual.items():
        if name not in expected:
            problems.append(f"{name}: generated but absent from {args.digests}")
        elif expected[name] != digest:
            problems.append(f"{name}: expected {expected[name]}, regenerated {digest}")
    problems.extend(
        f"{name}: present in {args.digests} but not generated"
        for name in expected
        if name not in actual
    )

    if problems:
        print(f"FIXTURE VERIFICATION FAILED ({len(problems)} problem(s)):", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print(
            "\nIf this change to the generator or the manifest is intended, run "
            "`make fixtures` and review the CORPUS.sha256 diff. A digest diff "
            "larger than the corpus.yaml diff means a shared random stream has "
            "coupled the fixtures together (determinism rule R1).",
            file=sys.stderr,
        )
        return 1

    print(f"{len(actual)} fixtures match {args.digests}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the corpus CLI.

    Args:
        argv: Argument vector, defaulting to ``sys.argv[1:]``.

    Returns:
        Process exit status.
    """
    args = build_parser().parse_args(argv)
    if args.command == "generate":
        return command_generate(args)
    return command_verify(args)


if __name__ == "__main__":
    sys.exit(main())
