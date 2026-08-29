"""Throwaway benchmark-only package (TEST-04, D-04) -- proves the chunked/bulk
``executemany()`` Oracle-write design is measurably faster than a genuine
naive row-by-row ``cursor.execute()`` loop, both driven by the identical
``csv_processor.engine.process_chunks()`` parse pass.

Deliberately lives at the repo root, NOT inside ``packages/csv-processor``
(never confused with the reusable engine) and NOT inside ``tests/`` (this is
a benchmark script, not a test that must pass/fail on every CI run) --
06-CONTEXT.md D-04.
"""

from __future__ import annotations
