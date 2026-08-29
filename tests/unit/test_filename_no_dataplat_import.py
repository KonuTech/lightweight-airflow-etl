"""Proves `detect/filename.py` has zero `dataplat` import (WR-01/G-03-3
regression) -- CLAUDE.md's "port logic in by rewriting, never by importing"
rule. The file's own docstring/comment prose mentions `dataplat.diagnostics`/
`dataplat.config.model` as plain text describing what was ported from --
those mentions are inside a `\"\"\"...\"\"\"` docstring or a `#` comment,
never at the start of a code line, so the check below anchors on line start
after stripping to never false-trigger on them.
"""

from __future__ import annotations

import datetime
import re
from pathlib import Path

import pytest

from csv_processor.detect.filename import FilenameMaskConfig, parse_filename
from csv_processor.errors import FilenameParsingError

_FILENAME_MODULE_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "packages"
    / "csv-processor"
    / "src"
    / "csv_processor"
    / "detect"
    / "filename.py"
)

_DATAPLAT_IMPORT_RE = re.compile(r"\s*(from dataplat|import dataplat)\b")


def test_no_dataplat_import_statement() -> None:
    source_text = _FILENAME_MODULE_PATH.read_text()
    for line in source_text.splitlines():
        assert _DATAPLAT_IMPORT_RE.match(line) is None, f"found dataplat import: {line!r}"


def test_filename_mask_config_is_real_and_importable() -> None:
    config = FilenameMaskConfig(mask="{dataset}_{business_date:%Y%m%d}.csv")

    result = parse_filename(config, "customers_20260829.csv")
    assert result["business_date"] == datetime.date(2026, 8, 29)

    with pytest.raises(FilenameParsingError):
        parse_filename(config, "not_a_match.csv")
