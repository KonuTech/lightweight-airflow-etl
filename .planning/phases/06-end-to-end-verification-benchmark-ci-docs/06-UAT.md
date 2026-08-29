---
status: testing
phase: 06-end-to-end-verification-benchmark-ci-docs
source: [06-VERIFICATION.md]
started: 2026-08-30T01:35:00Z
updated: 2026-08-30T01:35:00Z
---

## Current Test

number: 1
name: GitHub Actions CI actually runs on a real pull request
expected: |
  Both `lint-type-unit` and `oracle-e2e` jobs trigger automatically on the PR, run to completion
  (ruff/mypy/pytest for the first job; a real `docker compose up -d --wait` + `tests/e2e/` run
  against a fresh Oracle/Airflow stack on the ubuntu-latest runner for the second), and pass/fail
  status is visible on the PR's checks list — matching roadmap Success Criterion 3 literally, not
  just the workflow YAML's structural correctness.
awaiting: user response

## Tests

### 1. GitHub Actions CI actually runs on a real pull request
expected: Both `lint-type-unit` and `oracle-e2e` jobs trigger automatically, run to completion, and pass/fail status is visible on the PR's checks list.
result: [pending]

### 2. GitHub Branch Protection configuration
expected: In repo Settings → Branches, a protection rule for `main`/`master` requires `lint-type-unit` and `oracle-e2e` as passing status checks before merge. A PR cannot merge while either check is red or still running.
result: [pending]

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
