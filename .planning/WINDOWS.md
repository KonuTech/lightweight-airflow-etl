---
schema_version: 1
open_count: 0
waived_count: 0
fixed_count: 1
total_count: 1
last_updated: 2026-08-29T20:12:32.240Z
---

# Broken Windows Ledger

> Cross-phase defect register. With `workflow.windows_enforce` enabled, `/gsd-ship` blocks while `open_count > 0`.
> Waive with `gsd-tools windows waive <id> "<reason>"` (reason required).
> Mark fixed with `gsd-tools windows fixed <id>`.

| id | phase | kind | file | line | description | status | reason | recorded_at | resolved_at |
|----|-------|------|------|------|-------------|--------|--------|-------------|-------------|
| 1 | 03 | deviation | packages/csv-processor/src/csv_processor/detect/encoding.py |  | prepare_source()'s unconditional codecs.lookup(enc_detection.encoding) raises LookupError when detect_encoding() legitimately returns source='undetermined' (its own documented never-raises contract) -- a content-dependent charset_normalizer/chardet corroboration edge case, out of scope for 03-08's CR-04 coverage-gate fix | fixed |  | 2026-08-29T15:00:05.065Z | 2026-08-29T20:12:32.240Z |

````json
[
  {
    "id": 1,
    "kind": "deviation",
    "phase": "03",
    "file": "packages/csv-processor/src/csv_processor/detect/encoding.py",
    "line": null,
    "description": "prepare_source()'s unconditional codecs.lookup(enc_detection.encoding) raises LookupError when detect_encoding() legitimately returns source='undetermined' (its own documented never-raises contract) -- a content-dependent charset_normalizer/chardet corroboration edge case, out of scope for 03-08's CR-04 coverage-gate fix",
    "status": "fixed",
    "reason": "",
    "recorded_at": "2026-08-29T15:00:05.065Z",
    "resolved_at": "2026-08-29T20:12:32.240Z"
  }
]
````
