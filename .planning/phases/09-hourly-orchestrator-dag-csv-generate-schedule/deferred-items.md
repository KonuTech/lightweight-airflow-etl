# Deferred Items — Phase 09

Out-of-scope discoveries logged during plan execution, per the executor's
SCOPE BOUNDARY rule (only auto-fix issues directly caused by the current
task's changes).

## Plan 09-02

- **Pre-existing mypy error in `tests/unit/dags/test_generate_schedule_helpers.py:44`**
  (introduced by Plan 09-01, commit `b965756`): `format_cascade_summary`'s
  parameter type is `dict[str, dict[str, int] | None]`, but the test passes a
  `dict[str, dict[str, int]]` literal with no `None` value present — mypy's
  invariant `dict` typing flags this as an incompatible argument type
  (`[arg-type]`). Confirmed via `uv run mypy .` (the canonical whole-repo
  command per `Makefile`'s `lint` target) both before and after Plan 09-02's
  changes — this file was not touched by Plan 09-02, so out of scope to fix
  here. Suggested fix for whoever picks this up: annotate the test's local
  variable as `dict[str, dict[str, int] | None]` or use `Mapping` per mypy's
  own suggestion.
