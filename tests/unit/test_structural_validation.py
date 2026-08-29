"""Proves corpus fixtures 9-16 (``structural`` category) against
``csv_processor.source.prepare_source``/``csv_processor.engine.process_chunks``
(03-03-PLAN.md Task 3) -- every header-level whole-file-reject rule
(missing/extra/duplicate declared column, no header row, empty file) and
the row-level ``WRONG_COLUMN_COUNT`` rule.

Fixtures 9/10/13/14/16 declare a header matching (or a deliberate
single-column deviation from) ``customers.json``/``orders.json``'s real
column set exactly, so each runs against the real, loaded dataset config.
Fixture 11's duplicate-column-name check fires inside ``detect_header``
itself, before any comparison against a config's declared columns, so
which real config is passed is immaterial to the outcome -- ``orders.json``
is used since the fixture's header is orders-shaped. Fixture 15's headerless
content is also orders-shaped.

``process_chunks()`` is a generator -- calling it does not execute anything
until iteration begins, so every ``StructuralValidationError`` assertion
below wraps ``list(process_chunks(...))``, not the bare call.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from csv_processor import source
from csv_processor.config.loader import load_config
from csv_processor.config.models import (
    ColumnSpec,
    CsvDialectConfig,
    DatasetConfig,
    OracleTargetSpec,
    ProcessingConfig,
)
from csv_processor.engine import process_chunks
from csv_processor.errors import StructuralValidationError

from generator.generate_csv import generate_rows, write_csv
from tools.corpus.generators import generate_fixture, stream_for
from tools.corpus.manifest import load_manifest_with_seed

_MANIFEST_PATH = Path("tests/fixtures/corpus.yaml")
_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_CUSTOMERS_PATH = _CONFIGS_DIR / "datasets" / "customers.json"
_ORDERS_PATH = _CONFIGS_DIR / "datasets" / "orders.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"


def _fixture_bytes(name: str) -> bytes:
    """Materialize one named fixture's bytes directly from the manifest."""
    manifest = load_manifest_with_seed(_MANIFEST_PATH)
    fixture = next(f for f in manifest.fixtures if f.name == name)
    rng = stream_for(manifest.master_seed, fixture.name)
    return generate_fixture(fixture, rng)


def _write_fixture(tmp_path: Path, name: str, filename: str) -> Path:
    csv_path = tmp_path / filename
    csv_path.write_bytes(_fixture_bytes(name))
    return csv_path


def _customers_config():
    return load_config(_CUSTOMERS_PATH, defaults_path=_DEFAULTS_PATH)


def _orders_config():
    return load_config(_ORDERS_PATH, defaults_path=_DEFAULTS_PATH)


def test_09_missing_column(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "09_missing_column", "customers_09.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _customers_config()))

    assert exc.value.context["error_code"] == "MISSING_REQUIRED_COLUMN"


def test_10_extra_unexpected_column(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "10_extra_unexpected_column", "customers_10.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _customers_config()))

    assert exc.value.context["error_code"] == "EXTRA_UNEXPECTED_COLUMN"


def test_11_duplicate_column_name(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "11_duplicate_column_name", "orders_11.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _orders_config()))

    assert exc.value.context["error_code"] == "DUPLICATE_COLUMN_NAME"


def test_12_wrong_column_count_row(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "12_wrong_column_count_row", "orders_12.csv")

    chunks = list(process_chunks(csv_path, _orders_config()))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 2
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert invalid_rows[0]["row_number"] == 2
    assert valid_rows[0]["order_id"] == "ORD0001"
    assert valid_rows[1]["order_id"] == "ORD0003"


def test_13_empty_file(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "13_empty_file", "orders_13.csv")

    with pytest.raises(StructuralValidationError) as exc:
        list(process_chunks(csv_path, _orders_config()))

    assert exc.value.context["error_code"] == "NO_HEADER_ROW"


def test_14_header_only_no_rows_yields_zero_chunks(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "14_header_only_no_rows", "orders_14.csv")

    assert list(process_chunks(csv_path, _orders_config())) == []


def test_15_no_header_row(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "15_no_header_row", "orders_15.csv")

    # 03-03-PLAN.md's own <behavior>: the exact error_code depends on
    # detect_header's scoring heuristic (NO_HEADER_ROW or
    # MISSING_REQUIRED_COLUMN are both acceptable) -- assert the exception
    # TYPE and whole-file rejection only, never one specific code here.
    with pytest.raises(StructuralValidationError):
        list(process_chunks(csv_path, _orders_config()))


def test_16_ragged_rows_and_blank_lines(tmp_path: Path) -> None:
    csv_path = _write_fixture(tmp_path, "16_ragged_rows_and_blank_lines", "orders_16.csv")

    chunks = list(process_chunks(csv_path, _orders_config()))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert len(valid_rows) == 2
    assert [row["order_id"] for row in valid_rows] == ["ORD0001", "ORD0004"]

    assert len(invalid_rows) == 4
    assert all(row["error_code"] == "WRONG_COLUMN_COUNT" for row in invalid_rows)
    assert [row["row_number"] for row in invalid_rows] == [2, 3, 4, 5]
    # Row 2 and row 5 are the blank lines -- structurally absent fields,
    # None rather than "" (D-05).
    assert invalid_rows[0]["order_id"] is None
    assert invalid_rows[3]["order_id"] is None
    # Row 3 is the short row (2 of 4 fields) -- present fields keep their
    # real values, only the structurally-absent trailing fields are None.
    assert invalid_rows[1]["order_id"] == "ORD0002"
    assert invalid_rows[1]["customer_id"] == "CUST002"
    assert invalid_rows[1]["order_date"] is None
    assert invalid_rows[1]["amount"] is None
    # Row 4 is the long row (5 of 4 fields) -- every declared field is
    # present, the extra 5th field is simply dropped by the header-keyed
    # dict build, but the row is still flagged since raw field count (5)
    # != header field count (4).
    assert invalid_rows[2]["order_id"] == "ORD0003"
    assert invalid_rows[2]["amount"] == "300.00"


def test_optional_column_absent_from_header_processes_successfully(tmp_path: Path) -> None:
    """CR-01/G-03-1 regression: a customers CSV that genuinely omits
    `signup_country` (`required: false` in customers.json's own shipped
    config) must process successfully, not raise MISSING_REQUIRED_COLUMN."""
    csv_path = tmp_path / "customers_optional_absent.csv"
    csv_path.write_text(
        "customer_id,name,country,birth_date,event_ts\n"
        "CUST001,Alice,US,1990-01-01,2026-01-01T00:00:00+0000\n"
    )

    chunks = list(process_chunks(csv_path, _customers_config()))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 1
    assert valid_rows[0]["customer_id"] == "CUST001"
    assert valid_rows[0]["signup_country"] is None


def _preamble_footer_config(*, has_footer: bool = True) -> DatasetConfig:
    """Fixture-local ad hoc config for the CR-02 preamble/footer/repeated-
    header regression test -- mirrors test_byte_level_hard.py's
    `_order_id_note_config()` pattern (3 required, non-nullable string
    columns matching this test's own literal CSV header exactly).

    `has_footer` defaults to `True` because every EXISTING call site in this
    file exercises genuine footer/malformed-row-at-boundary exclusion-
    eligibility scenarios that require footer detection active (FTR-01); new
    tests proving the opt-out default pass `has_footer=False` explicitly."""
    return DatasetConfig(
        dataset="preamble_footer",
        file_pattern="preamble_footer_*.csv",
        csv=CsvDialectConfig(has_footer=has_footer),
        columns=[
            ColumnSpec(name="customer_id", type="string", nullable=False, required=True),
            ColumnSpec(name="name", type="string", nullable=False, required=True),
            ColumnSpec(name="country", type="string", nullable=False, required=True),
        ],
        oracle=OracleTargetSpec(valid_table="pf_valid", invalid_table="pf_invalid"),
        processing=ProcessingConfig(chunk_size=10),
    )


def test_preamble_footer_and_repeated_header_rows_excluded_from_processing(
    tmp_path: Path,
) -> None:
    """CR-02/G-03-2 regression: a genuine metadata preamble line, a footer
    line, and a repeated interior header row must never appear in either
    `valid_rows` or `invalid_rows` -- only the 3 real data rows should."""
    csv_path = tmp_path / "preamble_footer.csv"
    csv_path.write_text(
        "Report generated 2026-08-29\n"
        "customer_id,name,country\n"
        "CUST001,Alice,US\n"
        "CUST002,Bob,UK\n"
        "customer_id,name,country\n"
        "CUST003,Carol,DE\n"
        "END OF REPORT\n"
    )
    config = _preamble_footer_config()

    chunks = list(process_chunks(csv_path, config))

    assert len(chunks) == 1
    valid_rows, invalid_rows = chunks[0]
    assert invalid_rows == []
    assert len(valid_rows) == 3
    assert [row["customer_id"] for row in valid_rows] == ["CUST001", "CUST002", "CUST003"]


def _large_id_name_config(*, has_footer: bool = True) -> DatasetConfig:
    """Fixture-local ad hoc config for the CR-03 sample-boundary regression
    test -- mirrors `_preamble_footer_config()`'s exact shape (2 required,
    non-nullable string columns matching this test's own literal CSV header
    exactly).

    `has_footer` defaults to `True` because every EXISTING call site in this
    file exercises genuine footer/malformed-row-at-boundary exclusion-
    eligibility scenarios that require footer detection active (FTR-01); new
    tests proving the opt-out default pass `has_footer=False` explicitly."""
    return DatasetConfig(
        dataset="large_id_name",
        file_pattern="large_id_name_*.csv",
        csv=CsvDialectConfig(has_footer=has_footer),
        columns=[
            ColumnSpec(name="id", type="string", nullable=False, required=True),
            ColumnSpec(name="name", type="string", nullable=False, required=True),
        ],
        oracle=OracleTargetSpec(valid_table="lin_valid", invalid_table="lin_invalid"),
        processing=ProcessingConfig(chunk_size=1000),
    )


def test_large_well_formed_file_loses_zero_rows_across_sample_boundary(
    tmp_path: Path,
) -> None:
    """CR-03 regression (03-REVIEW.md Critical finding): a well-formed CSV
    larger than `source.SAMPLE_BYTES` must lose zero rows -- the row whose
    bytes straddle the 64 KiB detection sample's byte cutoff is truncated
    mid-row WITHIN THE SAMPLE ONLY, which previously caused
    `detect_header()`'s footer-scoring walk to flag its absolute index as a
    false-positive footer row. `_filtered_rows()` then silently dropped the
    same row's real, complete, well-formed content in PASS 2 -- no
    `invalid_rows` entry, no exception, no count mismatch surfaced anywhere.
    """
    lines = ["id,name"] + [f"ID{i:06d},Name{i:06d}" for i in range(1, 6001)]
    csv_path = tmp_path / "large_id_name.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    # Sanity check: the repro scenario is genuinely reproduced only when the
    # file actually exceeds the bounded detection sample.
    assert csv_path.stat().st_size > source.SAMPLE_BYTES
    config = _large_id_name_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert all_invalid == []
    assert len(all_valid) == 6000
    assert {row["id"] for row in all_valid} == {f"ID{i:06d}" for i in range(1, 6001)}


def test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped(
    tmp_path: Path,
) -> None:
    """CR-04 regression (03-REVIEW.md Critical finding): a genuinely
    malformed data row landing at the sample's own tail-adjacent position in
    a file larger than `source.SAMPLE_BYTES` must surface as a
    `WRONG_COLUMN_COUNT` invalid row, never silently vanish from both
    `valid_rows` and `invalid_rows`. CR-03's content re-validation alone
    cannot distinguish this case from a well-formed row truncated by the
    sample cutoff -- both independently satisfy the same field-count-
    mismatch criterion against their own real content. This is the sample's
    own LAST parsed row, always ineligible for exclusion under the new
    `sample_covered_row_count` coverage gate whenever the sample was
    truncated."""
    header = "id,name"
    lines = [header]
    cumulative = len(header) + 1
    good_ids: list[str] = []
    i = 1
    while cumulative <= source.SAMPLE_BYTES:
        row = f"ID{i:06d},Name{i:06d}"
        lines.append(row)
        cumulative += len(row) + 1
        good_ids.append(f"ID{i:06d}")
        i += 1
    # The loop's LAST appended row is -- by construction -- the one whose
    # real bytes straddle `source.SAMPLE_BYTES` (its start offset was
    # <= SAMPLE_BYTES, its end pushed cumulative past it). Replace it with a
    # genuinely malformed row (a real structural defect, not a truncation
    # artifact): a single field, no comma.
    good_ids.pop()
    lines[-1] = "BADROW_ONLY_ONE_FIELD"
    for _ in range(3000):
        row = f"ID{i:06d},Name{i:06d}"
        lines.append(row)
        good_ids.append(f"ID{i:06d}")
        i += 1
    csv_path = tmp_path / "malformed_at_boundary.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    assert csv_path.stat().st_size > source.SAMPLE_BYTES
    config = _large_id_name_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert len(all_invalid) == 1
    assert all_invalid[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert all_invalid[0]["id"] == "BADROW_ONLY_ONE_FIELD"
    assert all_invalid[0]["name"] is None
    assert {row["id"] for row in all_valid} == set(good_ids)


def test_repeated_header_row_excluded_even_when_file_exceeds_sample_size(
    tmp_path: Path,
) -> None:
    """No regression of G-03-2: an interior repeated-header row within the
    64 KiB detection sample must still be correctly excluded even when the
    surrounding file's total size exceeds the sample -- proving CR-03's
    re-validation fix and G-03-2's original exclusion guarantee hold
    simultaneously in one file."""
    lines = [
        "customer_id,name,country",
        "CUST001,Alice,US",
        "CUST002,Bob,UK",
        "customer_id,name,country",
    ]
    lines.extend(f"CUSTF{i:05d},Filler{i:05d},XX" for i in range(3000))
    csv_path = tmp_path / "repeated_header_large.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    assert csv_path.stat().st_size > source.SAMPLE_BYTES
    config = _preamble_footer_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert all_invalid == []
    assert len(all_valid) == 2 + 3000
    assert "customer_id" not in {row["customer_id"] for row in all_valid}
    assert {row["customer_id"] for row in all_valid} == {
        "CUST001",
        "CUST002",
        *(f"CUSTF{i:05d}" for i in range(3000)),
    }


def test_repeated_header_excluded_and_out_of_coverage_malformed_row_surfaced_together(
    tmp_path: Path,
) -> None:
    """Proves G-03-2's exclusion guarantee and CR-04's coverage-eligibility
    fix hold simultaneously in the same file: an interior repeated-header
    row (well within `sample_covered_row_count`'s provably-covered range) is
    excluded from both streams, while an out-of-coverage genuinely malformed
    row (the sample's own tail-adjacent position) surfaces as an ordinary
    `WRONG_COLUMN_COUNT` invalid row."""
    lines = [
        "customer_id,name,country",
        "CUST001,Alice,US",
        "CUST002,Bob,UK",
        "customer_id,name,country",
    ]
    cumulative = sum(len(line) + 1 for line in lines)
    good_customer_ids: list[str] = ["CUST001", "CUST002"]
    i = 0
    while cumulative <= source.SAMPLE_BYTES:
        row = f"CUSTF{i:05d},Filler{i:05d},XX"
        lines.append(row)
        cumulative += len(row) + 1
        good_customer_ids.append(f"CUSTF{i:05d}")
        i += 1
    # The loop's LAST appended row is -- by construction -- the one whose
    # real bytes straddle `source.SAMPLE_BYTES`. Replace it with a genuinely
    # malformed row (single field, no commas). Deliberately no underscores
    # here (unlike Task 1's literal) -- an underscore-containing literal at
    # this exact position tips charset_normalizer's ascii-vs-utf-8 pick for
    # this specific sample's byte distribution, an unrelated pre-existing
    # detect/encoding.py quirk out of this plan's scope; this equivalent
    # single-field malformed literal avoids it.
    good_customer_ids.pop()
    lines[-1] = "BADROWONLYONEFIELD"
    for _ in range(1000):
        row = f"CUSTF{i:05d},Filler{i:05d},XX"
        lines.append(row)
        good_customer_ids.append(f"CUSTF{i:05d}")
        i += 1
    csv_path = tmp_path / "repeated_header_and_malformed.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    assert csv_path.stat().st_size > source.SAMPLE_BYTES
    config = _preamble_footer_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert len(all_invalid) == 1
    assert all_invalid[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert all_invalid[0]["customer_id"] == "BADROWONLYONEFIELD"
    assert "customer_id" not in {row["customer_id"] for row in all_valid}
    assert {row["customer_id"] for row in all_valid} == set(good_customer_ids)


def test_uncoverable_tail_indices_covers_adversarial_edge_cases() -> None:
    """CR-01: `_uncoverable_tail_indices()` computes the maximal contiguous
    suffix of a SINGLE candidate-index set ending at `sample_covered_row_count`
    (the sample's own unprovable last row) -- proven directly against every
    adversarial boundary condition identified before implementation, fully
    decoupled from any CSV-level byte-position engineering."""
    cases: list[tuple[set[int], int, set[int]]] = [
        (set(), 10, set()),  # empty excluded_indices
        ({5}, 5, {5}),  # single index exactly at the boundary
        ({3, 4, 5}, 5, {3, 4, 5}),  # contiguous run of 3 touching the boundary
        # a run touching the boundary is captured, a separate non-adjacent
        # run ({1, 2}) is left untouched
        ({1, 2, 7, 8}, 8, {7, 8}),
        # run touching absolute index 0, must not underflow/loop forever
        ({0, 1, 2}, 2, {0, 1, 2}),
        ({5}, 0, set()),  # boundary at row 0, index 5 not adjacent -- no false positive
        ({0}, 0, {0}),  # boundary at row 0 AND index 0 is itself a candidate
        # index 6 does not touch a boundary of 5 -- walk starts at 5, 5 not
        # in {6}, stops immediately, 6 is never even considered
        ({6}, 5, set()),
    ]
    for excluded_indices, sample_covered_row_count, expected in cases:
        result = source._uncoverable_tail_indices(excluded_indices, sample_covered_row_count)
        assert result == expected, (
            f"_uncoverable_tail_indices({excluded_indices!r}, "
            f"{sample_covered_row_count!r}) == {result!r}, expected {expected!r}"
        )


def test_two_contiguous_malformed_rows_at_sample_boundary_both_surface_as_invalid(
    tmp_path: Path,
) -> None:
    """CR-01 regression (03-REVIEW.md Critical finding, fourth review round):
    a contiguous run of TWO genuinely malformed rows at the sample's own
    tail-adjacent position must BOTH surface as `WRONG_COLUMN_COUNT` invalid
    rows -- `detect/header.py`'s `_detect_footer_rows` walks backward and
    chains contiguous field-count mismatches together, but 03-08's own CR-04
    fix only protected the single index equal to `sample_covered_row_count`;
    every other index in the same contiguous run was left eligible for CR-03's
    content re-validation alone, which cannot distinguish a genuine footer
    from a genuinely malformed row caught in the same chain."""
    header = "id,name"
    lines = [header]
    cumulative = len(header) + 1
    good_ids: list[str] = []
    i = 1
    while cumulative <= source.SAMPLE_BYTES:
        row = f"ID{i:06d},Name{i:06d}"
        lines.append(row)
        cumulative += len(row) + 1
        good_ids.append(f"ID{i:06d}")
        i += 1
    # The loop's last TWO appended rows straddle the sample's byte cutoff.
    # Replace both with genuinely malformed single-field literals of EXACTLY
    # the same length (19 chars) as the well-formed row format they replace,
    # keeping the file's total byte layout byte-for-byte identical to what
    # `cumulative` already computed -- no underscores (03-08's own documented
    # `detect/encoding.py` corroboration-sensitivity deviation).
    good_ids.pop()
    good_ids.pop()
    lines[-1] = "BADROWNUMBERONE1234"
    lines[-2] = "BADROWNUMBERTWO5678"
    for _ in range(3000):
        row = f"ID{i:06d},Name{i:06d}"
        lines.append(row)
        good_ids.append(f"ID{i:06d}")
        i += 1
    csv_path = tmp_path / "two_contiguous_malformed_at_boundary.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    assert csv_path.stat().st_size > source.SAMPLE_BYTES
    config = _large_id_name_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert len(all_invalid) == 2
    assert {row["id"] for row in all_invalid} == {
        "BADROWNUMBERONE1234",
        "BADROWNUMBERTWO5678",
    }
    for invalid_row in all_invalid:
        assert invalid_row["error_code"] == "WRONG_COLUMN_COUNT"
        assert invalid_row["name"] is None
    assert {row["id"] for row in all_valid} == set(good_ids)


def test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run(
    tmp_path: Path,
) -> None:
    """Checker-found regression: the per-source-then-union fix must not let
    a boundary-touching footer-shaped run (`footer_row_indices`) swallow an
    unrelated INTERIOR repeated-header row (`repeated_header_row_indices`)
    merely because their absolute indices are numerically adjacent by pure
    coincidence -- `_detect_footer_rows`'s contiguous backward walk and
    `_detect_repeated_header_rows`'s unbounded full-scan are structurally
    independent detectors with no shared ordering guarantee. Under a naive
    union-then-walk implementation, the genuine interior repeated-header row
    is incorrectly stripped of its exclusion-eligibility and leaks through to
    ordinary per-row validation as an unremarkable VALID row (its own content
    -- the literal header values -- happens to satisfy every structural/type/
    nullability check for this config's plain string columns), instead of
    being excluded from both streams per G-03-2."""
    header = "id,name"
    lines = [header]
    cumulative = len(header) + 1
    good_ids: list[str] = []
    i = 1
    while cumulative <= source.SAMPLE_BYTES:
        row = f"ID{i:06d},Name{i:06d}"
        lines.append(row)
        cumulative += len(row) + 1
        good_ids.append(f"ID{i:06d}")
        i += 1
    # lines[-1] (absolute index sample_covered_row_count) is boundary-touching
    # and footer-shaped (flagged only by footer_row_indices). lines[-2]
    # (absolute index sample_covered_row_count - 1, genuinely INTERIOR) is an
    # exact duplicate of the header (flagged only by repeated_header_row_indices'
    # own unrelated unbounded scan).
    good_ids.pop()
    good_ids.pop()
    lines[-1] = "BADROWNUMBERONE1234"
    lines[-2] = "id,name"
    for _ in range(3000):
        row = f"ID{i:06d},Name{i:06d}"
        lines.append(row)
        good_ids.append(f"ID{i:06d}")
        i += 1
    csv_path = tmp_path / "interior_repeated_header_adjacent_boundary.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    assert csv_path.stat().st_size > source.SAMPLE_BYTES
    config = _large_id_name_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert len(all_invalid) == 1
    assert all_invalid[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert all_invalid[0]["id"] == "BADROWNUMBERONE1234"
    assert all_invalid[0]["name"] is None
    valid_ids = {row["id"] for row in all_valid}
    assert "id" not in valid_ids
    assert valid_ids == set(good_ids)


def test_file_exactly_sample_bytes_size_footer_still_correctly_excluded(
    tmp_path: Path,
) -> None:
    """WR-01 regression (03-REVIEW.md Warning finding): a file whose real
    byte size exactly equals `source.SAMPLE_BYTES` must never be
    misclassified as truncated. `sample_was_truncated = len(sample) ==
    SAMPLE_BYTES` cannot distinguish "the read was cut off because more file
    follows" from "the file's real size happens to equal SAMPLE_BYTES
    exactly, and the read simply reached true EOF" -- both produce the
    identical `len(sample) == SAMPLE_BYTES` result. In the second case this
    wrongly strips coverage-eligibility from that file's genuinely complete
    last row, so a real footer there fails to be excluded and instead
    surfaces as a spurious `WRONG_COLUMN_COUNT` invalid row."""
    header = "id,name"
    footer_row = "ENDOFFILEMARKER"
    footer_line_bytes = len(footer_row) + 1
    lines = [header]
    good_ids: list[str] = []
    i = 1
    while True:
        row = f"ID{i:06d},Name{i:06d}"
        prospective_total = sum(len(line) + 1 for line in lines) + len(row) + 1 + footer_line_bytes
        if prospective_total > source.SAMPLE_BYTES:
            break
        lines.append(row)
        good_ids.append(f"ID{i:06d}")
        i += 1
    shortfall = source.SAMPLE_BYTES - (sum(len(line) + 1 for line in lines) + footer_line_bytes)
    assert shortfall >= 0
    lines[-1] = lines[-1] + ("X" * shortfall)
    lines.append(footer_row)
    csv_path = tmp_path / "exact_sample_bytes.csv"
    csv_path.write_text("\n".join(lines) + "\n")
    assert csv_path.stat().st_size == source.SAMPLE_BYTES
    config = _large_id_name_config()

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert all_invalid == []
    assert {row["id"] for row in all_valid} == set(good_ids)


def test_no_footer_optin_default_surfaces_malformed_last_row_as_invalid_not_dropped(
    tmp_path: Path,
) -> None:
    """FTR-01 regression (03-VERIFICATION.md new gap finding): a dataset that
    never opts in to `has_footer` must NEVER exclude its genuine last row on
    field-count-mismatch grounds alone, at any file size -- a genuinely
    malformed final row always surfaces as a `WRONG_COLUMN_COUNT` invalid
    row. This is a small, well-within-`SAMPLE_BYTES` file (no truncation
    involved at all) -- the verifier's own primary, simplest reproduction of
    the baseline defect (distinct from the 03-06..03-09 sample-boundary-
    truncation chain)."""
    csv_path = tmp_path / "no_footer_optin.csv"
    csv_path.write_text(
        "id,name\nID000001,Name000001\nID000002,Name000002\nMALFORMEDLASTROWNOCOMMA\n"
    )
    assert csv_path.stat().st_size < source.SAMPLE_BYTES
    config = _large_id_name_config(has_footer=False)

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert len(all_invalid) == 1
    assert all_invalid[0]["error_code"] == "WRONG_COLUMN_COUNT"
    assert all_invalid[0]["id"] == "MALFORMEDLASTROWNOCOMMA"
    assert all_invalid[0]["name"] is None
    assert {row["id"] for row in all_valid} == {"ID000001", "ID000002"}


def test_generator_driven_customers_seed11_wrong_column_count_last_row_not_dropped(
    tmp_path: Path,
) -> None:
    """FTR-01 regression: re-derives 03-VERIFICATION.md's own real-generator/
    real-`customers.json` reproduction exactly. Seed=11 (50 rows, 30% invalid
    ratio) places a `wrong_column_count` invalid row at the generator's own
    physical last position -- the generator reports 50 rows (35 valid, 15
    invalid) but, prior to this fix, `process_chunks()` only accounted for 49
    (35 valid, 14 invalid): the last row vanished with zero trace in either
    stream."""
    config = _customers_config()
    generated = generate_rows(config, rows=50, invalid_ratio=0.3, seed=11)
    # Sanity precondition re-deriving the verifier's own finding that this
    # exact seed/row-count/ratio combination places a wrong_column_count row
    # at the file's true last position.
    assert generated.categories[-1] == "wrong_column_count"
    csv_path = tmp_path / "customers_seed11.csv"
    write_csv(generated, config, csv_path)

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    expected_valid = sum(1 for c in generated.categories if c is None)
    expected_invalid = sum(1 for c in generated.categories if c is not None)
    assert expected_valid == 35
    assert expected_invalid == 15
    assert len(all_valid) == expected_valid
    assert len(all_invalid) == expected_invalid
    assert len(all_valid) + len(all_invalid) == 50


def test_footer_optin_still_excludes_genuine_footer_row_within_sample(
    tmp_path: Path,
) -> None:
    """FTR-01 regression (adversarial "whole file fits in sample AND a genuine
    footer is declared" case): a dataset that explicitly opts in via
    `has_footer=True` must still correctly exclude its genuine trailing
    footer row, identically to the pre-existing unconditional-heuristic
    behavior -- this is a permanent regression proof, not a RED/GREEN pair
    (it passes both before and after this plan's own fix)."""
    csv_path = tmp_path / "footer_optin.csv"
    csv_path.write_text("id,name\nID000001,Name000001\nID000002,Name000002\nTOTALSROWNOTREALDATA\n")
    assert csv_path.stat().st_size < source.SAMPLE_BYTES
    config = _large_id_name_config(has_footer=True)

    chunks = list(process_chunks(csv_path, config))

    all_valid = [row for valid_rows, _ in chunks for row in valid_rows]
    all_invalid = [row for _, invalid_rows in chunks for row in invalid_rows]
    assert all_invalid == []
    assert len(all_valid) == 2
    valid_ids = {row["id"] for row in all_valid}
    assert valid_ids == {"ID000001", "ID000002"}
    assert "TOTALSROWNOTREALDATA" not in valid_ids
