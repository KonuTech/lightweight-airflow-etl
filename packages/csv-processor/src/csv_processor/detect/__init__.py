"""Detection package (D-25): Tier-A vendored dialect/encoding/header sniffing.

Only the functions later plans in this phase actually call are re-exported here
(``detect_dialect``/``to_stdlib_dialect``, ``detect_encoding``/``decode_strict``,
``detect_header``). ``filename.py``/``schema.py`` are vendored for parity (D-27)
but have no caller in this config-driven design -- they stay importable via their
own submodule paths only (``csv_processor.detect.filename``,
``csv_processor.detect.schema``), matching ``csv_processor.config``'s own
``__all__`` pattern.
"""

from __future__ import annotations

from csv_processor.detect.dialect import detect_dialect, to_stdlib_dialect
from csv_processor.detect.encoding import decode_strict, detect_encoding
from csv_processor.detect.header import detect_header

__all__ = [
    "decode_strict",
    "detect_dialect",
    "detect_encoding",
    "detect_header",
    "to_stdlib_dialect",
]
