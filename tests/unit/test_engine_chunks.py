"""End-to-end tracer: one valid, one invalid ``customers`` row through the
complete detect -> parse -> validate -> normalize -> split pipeline
(03-03-PLAN.md Task 1 -- the plan's own leading tracer slice).

Proves ``csv_processor.engine.process_chunks()`` wires ``source.py``,
``validate.py``, and ``normalize.py`` together correctly for the first
time in this phase, against a real, loaded ``customers.json`` config --
not just "no exception raised".

03-05-PLAN.md Task 1 extends this file with two more proofs against the
already-built ``process_chunks()``:

- Chunk-boundary / cross-chunk ``row_number`` continuity: an ad hoc 12-row
  fixture with ``chunk_size=5`` proves ``row_number`` counts sequentially
  ``1..12`` across all 3 chunk boundaries, never resetting per chunk.
- ENGINE-07's bounded-memory guarantee, empirically: reusing
  ``tests/unit/test_corpus_bounded_memory.py``'s exact ``RLIMIT_AS``/
  subprocess technique, but calling ``process_chunks()`` against the
  corpus's real ~60 MiB fixture 28 instead of a raw line iterator.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
from pathlib import Path

import pytest

from csv_processor.config.loader import load_config
from csv_processor.config.models import (
    ColumnSpec,
    CsvDialectConfig,
    DatasetConfig,
    OracleTargetSpec,
    ProcessingConfig,
)
from csv_processor.engine import process_chunks

try:
    import resource
except ImportError:  # pragma: no cover - non-POSIX platform (e.g. Windows)
    resource = None  # type: ignore[assignment]

from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"

_TRACER_CSV = (
    "customer_id,name,country,birth_date,event_ts,signup_country\n"
    "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
    ",Bob Jones,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
)


def test_one_valid_one_invalid_customers_row_end_to_end(tmp_path: Path) -> None:
    csv_path = tmp_path / "customers_20260829.csv"
    csv_path.write_text(_TRACER_CSV, encoding="utf-8")
    config = load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 1
    assert len(invalid_rows) == 1

    valid_row = valid_rows[0]
    assert valid_row == {
        "customer_id": "CUST001",
        "name": "Alice Smith",
        "country": "DE",
        "birth_date": dt.date(1990, 1, 1),
        "event_ts": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        "signup_country": "FR",
    }
    assert isinstance(valid_row["birth_date"], dt.date)
    assert not isinstance(valid_row["birth_date"], dt.datetime)
    assert isinstance(valid_row["event_ts"], dt.datetime)

    invalid_row = invalid_rows[0]
    assert invalid_row["error_code"] == "NULL_VIOLATION"
    assert invalid_row["customer_id"] == ""
    assert invalid_row["row_number"] == 2
    assert invalid_row["source_file"] == "customers_20260829.csv"
    assert "raw_line" in invalid_row
    assert invalid_row["raw_line"].startswith(",Bob Jones,DE,")


def test_structurally_broken_row_never_reaches_check_row(tmp_path: Path) -> None:
    """A wrong-field-count row's error_code is always WRONG_COLUMN_COUNT,
    never a type/nullability code -- proves the D-13 short-circuit.

    Two well-formed 6-field rows surround the ragged 3-field row so
    ``detect_header``'s modal-field-count heuristic still recognizes row 0
    as the header (it needs real following-row context, per
    03-RESEARCH.md's own header-detection note) -- a lone ragged row with
    nothing else to compare against would otherwise be misdetected as the
    header itself.
    """
    csv_path = tmp_path / "customers_ragged.csv"
    csv_path.write_text(
        "customer_id,name,country,birth_date,event_ts,signup_country\n"
        "CUST001,Alice Smith,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n"
        "CUST002,Bob Jones,DE\n"
        "CUST003,Carol White,DE,1990-01-01,2026-01-01T00:00:00+0000,FR\n",
        encoding="utf-8",
    )
    config = load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 2
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert invalid_rows[0]["row_number"] == 2


def test_convert_value_decimal_precision_exceeded() -> None:
    from csv_processor.config.models import ColumnSpec
    from csv_processor.normalize import convert_value

    column = ColumnSpec(
        name="amount", type="decimal", nullable=True, required=True, precision=12, scale=2
    )
    value, error_code = convert_value("100.999", column)

    assert value is None
    assert error_code == "DECIMAL_PRECISION_EXCEEDED"


def test_convert_value_invalid_date_format() -> None:
    from csv_processor.config.models import ColumnSpec
    from csv_processor.normalize import convert_value

    column = ColumnSpec(
        name="birth_date", type="date", nullable=True, required=True, format="%Y-%m-%d"
    )
    value, error_code = convert_value("31/02/2026", column)

    assert value is None
    assert error_code == "INVALID_DATE_FORMAT"


# ---------------------------------------------------------------------------
# 03-05-PLAN.md Task 1: chunk boundaries + cross-chunk row_number continuity
# ---------------------------------------------------------------------------


def test_chunk_boundaries_and_cross_chunk_row_number_continuity(tmp_path: Path) -> None:
    """12 data rows at ``chunk_size=5`` -> exactly 3 chunks, sizes [5, 5, 2],
    and ``row_number`` forms the literal sequence 1..12 across all 3 chunks
    with no resets at a chunk boundary.

    Every one of the 12 rows is made deliberately invalid (an empty
    ``id``, which ``id``'s ``nullable=False`` rejects) so ``row_number`` --
    only ever present on an invalid-row dict (D-09), never on a valid row's
    typed dict -- is directly inspectable for the FULL sequence, not just a
    sampled subset.
    """
    config = DatasetConfig.model_validate(
        {
            "dataset": "chunktest",
            "file_pattern": "chunktest_*.csv",
            "columns": [
                {"name": "id", "type": "string", "nullable": False, "required": True},
                {"name": "value", "type": "string", "nullable": True, "required": True},
            ],
            "oracle": {"valid_table": "chunktest_valid", "invalid_table": "chunktest_invalid"},
            "processing": {"chunk_size": 5},
        }
    )

    lines = ["id,value"] + [f",value{i}" for i in range(1, 13)]
    csv_path = tmp_path / "chunktest.csv"
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 3
    chunk_sizes = [len(valid_rows) + len(invalid_rows) for valid_rows, invalid_rows in chunks]
    assert chunk_sizes == [5, 5, 2]

    row_numbers: list[object] = []
    for valid_rows, invalid_rows in chunks:
        assert valid_rows == []  # every row is deliberately invalid
        assert all(row["error_code"] == "NULL_VIOLATION" for row in invalid_rows)
        row_numbers.extend(row["row_number"] for row in invalid_rows)

    assert row_numbers == list(range(1, 13))


# ---------------------------------------------------------------------------
# 03-05-PLAN.md Task 1: ENGINE-07 bounded-memory guarantee, proven
# empirically against the corpus's real ~60 MiB fixture 28, reusing
# test_corpus_bounded_memory.py's exact RLIMIT_AS/subprocess technique.
# ---------------------------------------------------------------------------

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")
_LARGE_FIXTURE_NAME = "28_large_streaming_profile"
_LARGE_FIXTURE_PATH = Path("tests/fixtures/csv") / _LARGE_FIXTURE_NAME

# 128 MiB (134,217,728 bytes) -- raised from this module's original 100 MiB
# (104,857,600-byte) cap during 04-02-PLAN.md Task 3.
#
# `from csv_processor.engine import process_chunks` now also transitively
# imports `csv_processor.load` (Plan 04-01) -- and, through it, the
# `oracledb` driver -- because engine.py's own `process()` (ENGINE-08, this
# phase's public entrypoint) is a module-level function in the SAME file,
# and `csv_processor.engine.load.get_connection` must be patchable as a
# plain module attribute for tests/unit/test_engine_process.py's mocking to
# work (a lazy, function-local `import oracledb`/`from csv_processor import
# load` inside `process()` was tried and rejected for exactly this reason --
# it breaks `patch("csv_processor.engine.load.get_connection")`). This adds
# genuine import-time memory overhead on top of the pydantic/pydantic-core,
# clevercsv, charset-normalizer, chardet stack the original 100 MiB comment
# already accounted for -- verified empirically this session via a
# standalone probe script: 100/110/120 MiB all die with a `MemoryError`
# during the real file-streaming read (not at import time), while 125 MiB
# is flaky (1 failure in 3 runs) and 128 MiB reliably succeeds (3/3 runs).
# The buffering negative control below still reliably dies with a
# ``MemoryError`` under this SAME 128 MiB cap (re-verified this session) --
# so 134_217_728 is the smallest empirically-verified cap that keeps this
# pair of tests meaningful (streaming survives, buffering does not, same
# cap) now that `process_chunks()` and `process()` share one module.
_RLIMIT_AS_BYTES = 134_217_728

# Both scripts set their own RLIMIT_AS cap *inside* the subprocess, exactly
# like test_corpus_bounded_memory.py's own scripts -- setting it after the
# interpreter has already started only bounds further growth, which is
# what makes the streaming/buffering contrast meaningful here too.
_STREAMING_SCRIPT = """
import resource, sys
resource.setrlimit(resource.RLIMIT_AS, (int(sys.argv[2]), int(sys.argv[2])))
from pathlib import Path
from csv_processor.config.models import (
    ColumnSpec, CsvDialectConfig, DatasetConfig, OracleTargetSpec, ProcessingConfig,
)
from csv_processor.engine import process_chunks

config = DatasetConfig(
    dataset="large_streaming_profile",
    file_pattern="large_streaming_profile_*.csv",
    csv=CsvDialectConfig(),
    columns=[
        ColumnSpec(name="id", type="string", nullable=False, required=True),
        ColumnSpec(name="payload", type="string", nullable=False, required=True),
    ],
    oracle=OracleTargetSpec(valid_table="large_valid", invalid_table="large_invalid"),
    processing=ProcessingConfig(chunk_size=5000),
)

# This IS ENGINE-07's actual bounded-memory contract: only ever hold one
# chunk's rows, discard immediately -- never append a chunk to a growing
# list.
total = valid = invalid = 0
for valid_rows, invalid_rows in process_chunks(Path(sys.argv[1]), config):
    total += len(valid_rows) + len(invalid_rows)
    valid += len(valid_rows)
    invalid += len(invalid_rows)

assert total == 62915, total
assert invalid == 0, invalid
"""

_BUFFERING_SCRIPT = """
import resource, sys
resource.setrlimit(resource.RLIMIT_AS, (int(sys.argv[2]), int(sys.argv[2])))
from pathlib import Path
from csv_processor.config.models import (
    ColumnSpec, CsvDialectConfig, DatasetConfig, OracleTargetSpec, ProcessingConfig,
)
from csv_processor.engine import process_chunks

config = DatasetConfig(
    dataset="large_streaming_profile",
    file_pattern="large_streaming_profile_*.csv",
    csv=CsvDialectConfig(),
    columns=[
        ColumnSpec(name="id", type="string", nullable=False, required=True),
        ColumnSpec(name="payload", type="string", nullable=False, required=True),
    ],
    oracle=OracleTargetSpec(valid_table="large_valid", invalid_table="large_invalid"),
    processing=ProcessingConfig(chunk_size=5000),
)

# Negative control: accumulate every chunk's rows for the whole ~62,915-row
# file before ever summing anything -- the exact anti-pattern ENGINE-07
# forbids.
list(process_chunks(Path(sys.argv[1]), config))
"""


@pytest.fixture(scope="module")
def large_fixture_path() -> Path:
    """Ensure the real 28_large_streaming_profile fixture is materialized.

    Mirrors ``test_corpus_bounded_memory.py``'s own self-materializing
    fixture -- ``tests/fixtures/csv/**`` is gitignored (only the manifest +
    digest oracle are committed), so this generates the one fixture
    directly from the committed manifest if a prior ``make fixtures`` run
    hasn't already produced it.
    """
    if _LARGE_FIXTURE_PATH.is_file():
        return _LARGE_FIXTURE_PATH

    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == _LARGE_FIXTURE_NAME)
    rng = stream_for(manifest.master_seed, fixture.name)
    content = generate_fixture(fixture, rng)
    _LARGE_FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _LARGE_FIXTURE_PATH.write_bytes(content)
    return _LARGE_FIXTURE_PATH


def test_process_chunks_streaming_survives_the_rlimit_as_cap(
    large_fixture_path: Path,
) -> None:
    if resource is None:
        pytest.skip("RLIMIT_AS bounded-memory test requires a POSIX resource module")

    result = subprocess.run(
        [sys.executable, "-c", _STREAMING_SCRIPT, str(large_fixture_path), str(_RLIMIT_AS_BYTES)],
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")


def test_process_chunks_buffering_dies_under_the_identical_rlimit_as_cap(
    large_fixture_path: Path,
) -> None:
    """Negative control: the same fixture, the same cap, but accumulating
    every chunk into one growing list must fail (typically SIGKILL/137 or a
    MemoryError-carrying non-zero exit) -- proving the cap is actually
    constraining something, not vacuously passing either variant."""
    if resource is None:
        pytest.skip("RLIMIT_AS bounded-memory test requires a POSIX resource module")

    result = subprocess.run(
        [sys.executable, "-c", _BUFFERING_SCRIPT, str(large_fixture_path), str(_RLIMIT_AS_BYTES)],
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
