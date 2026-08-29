"""Regression test for a real LookupError crash found live during Phase 5
Plan 02's orders-dataset trigger (matches the open WINDOWS.md deviation
logged in Phase 3 Plan 08's `test_filename_no_dataplat_import.py`-adjacent
note about `detect/encoding.py`'s "undetermined" corroboration outcome).

``detect_encoding`` documents that ``EncodingDetection.encoding`` is the
literal string ``"undetermined"`` (never a real codec name) whenever
``source == "undetermined"``. ``prepare_source`` previously called
``codecs.lookup(enc_detection.encoding).name`` unconditionally, one line
before checking ``enc_detection.source`` -- so an undetermined result raised
an uncaught ``LookupError: unknown encoding: undetermined`` instead of
silently deferring to the dataset's configured encoding, as D-28 and the
function's own inline comment both already documented as the intended
behavior.
"""

from __future__ import annotations

from pathlib import Path

from csv_processor import source
from csv_processor.config.loader import load_config
from csv_processor.detect.encoding import EncodingDetection

_CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent / "configs"
_ORDERS_PATH = _CONFIGS_DIR / "datasets" / "orders.json"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"


def test_prepare_source_defers_to_config_when_encoding_undetermined(
    tmp_path: Path, monkeypatch
) -> None:
    """An "undetermined" encoding detection must never reach
    ``codecs.lookup`` -- ``prepare_source`` must silently defer to the
    dataset's configured encoding (D-28) instead of raising ``LookupError``.
    """
    config = load_config(_ORDERS_PATH, defaults_path=_DEFAULTS_PATH)
    csv_path = tmp_path / "orders_undetermined.csv"
    csv_path.write_text(
        "order_id,customer_id,order_date,amount\nORD-1,CUST-1,2026-01-01,12.34\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        source.detect,
        "detect_encoding",
        lambda sample, *, contract_encoding: EncodingDetection("undetermined", 0.0, "undetermined"),
    )

    # Must not raise LookupError -- proves the fix, not just a passing status.
    text_stream, paired_rows, header = source.prepare_source(csv_path, config)
    try:
        rows = list(paired_rows)
    finally:
        text_stream.close()

    assert header == ("order_id", "customer_id", "order_date", "amount")
    assert len(rows) == 1
