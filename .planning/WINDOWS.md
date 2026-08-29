---
schema_version: 1
open_count: 1
waived_count: 0
fixed_count: 1
total_count: 2
last_updated: 2026-08-29T22:55:31.119Z
---

# Broken Windows Ledger

> Cross-phase defect register. With `workflow.windows_enforce` enabled, `/gsd-ship` blocks while `open_count > 0`.
> Waive with `gsd-tools windows waive <id> "<reason>"` (reason required).
> Mark fixed with `gsd-tools windows fixed <id>`.

| id | phase | kind | file | line | description | status | reason | recorded_at | resolved_at |
|----|-------|------|------|------|-------------|--------|--------|-------------|-------------|
| 1 | 03 | deviation | packages/csv-processor/src/csv_processor/detect/encoding.py |  | prepare_source()'s unconditional codecs.lookup(enc_detection.encoding) raises LookupError when detect_encoding() legitimately returns source='undetermined' (its own documented never-raises contract) -- a content-dependent charset_normalizer/chardet corroboration edge case, out of scope for 03-08's CR-04 coverage-gate fix | fixed |  | 2026-08-29T15:00:05.065Z | 2026-08-29T20:12:32.240Z |
| 2 | 06 | unrun-verify | .github/workflows/ci.yml |  | Branch Protection must separately name lint-type-unit/oracle-e2e as required status checks (Pitfall 6) -- a manual, repo-admin-only GitHub setting, not verifiable from any workflow YAML or automated command | open |  | 2026-08-29T22:55:31.119Z |  |

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
  },
  {
    "id": 2,
    "kind": "unrun-verify",
    "phase": "06",
    "file": ".github/workflows/ci.yml",
    "line": null,
    "description": "Branch Protection must separately name lint-type-unit/oracle-e2e as required status checks (Pitfall 6) -- a manual, repo-admin-only GitHub setting, not verifiable from any workflow YAML or automated command",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-29T22:55:31.119Z",
    "resolved_at": null
  }
]
````
