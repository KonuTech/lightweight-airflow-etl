---
status: partial
phase: 06-end-to-end-verification-benchmark-ci-docs
source: [06-VERIFICATION.md]
started: 2026-08-30T01:35:00Z
updated: 2026-08-30T07:15:00Z
---

## Current Test

number: 2
name: GitHub Branch Protection configuration
expected: |
  In repo Settings → Branches, a protection rule for `main`/`master` requires `lint-type-unit`
  and `oracle-e2e` as passing status checks before merge. A PR cannot merge while either check
  is red or still running.
awaiting: user response

## Tests

### 1. GitHub Actions CI actually runs on a real pull request
expected: Both `lint-type-unit` and `oracle-e2e` jobs trigger automatically, run to completion, and pass/fail status is visible on the PR's checks list.
result: pass
note: |
  Confirmed live on PR #1 (https://github.com/KonuTech/lightweight-airflow-etl/pull/1) after
  fixing 4 real bugs discovered by this exact test, none of which were caught by prior local-only
  verification (Phase 5's own evidence capture ran against an already-warm stack that masked all
  four):
  1. simple_auth_manager_passwords.json.generated needed chmod 666 in CI (uid 50000 PermissionError)
  2. data/customers, data/orders needed pre-creation before docker compose (root-owned bind mount)
  3. wait_for_task_state's 60s default was tight for cold-stack scheduling latency (bumped to 180s)
  4. ROOT CAUSE: AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION defaults to true -- a genuinely fresh
     metadata DB pauses csv_ingest the instant it's first parsed, and the scheduler schedules zero
     task instances for a paused DAG's run, even a manually/API-triggered one. Fixed at the source
     in docker-compose.yml. This would have silently broken DOC-01's "fresh clone to a completed
     HTTP-triggered ingestion" claim for every real new developer, not just CI.
  Final run: https://github.com/KonuTech/lightweight-airflow-etl/actions/runs/33298629045 --
  lint-type-unit pass (23s), oracle-e2e pass (3m27s), both visible on the PR's checks list.

### 2. GitHub Branch Protection configuration
expected: In repo Settings → Branches, a protection rule for `main`/`master` requires `lint-type-unit` and `oracle-e2e` as passing status checks before merge. A PR cannot merge while either check is red or still running.
result: [pending]

## Summary

total: 2
passed: 1
issues: 0
pending: 1
skipped: 0
blocked: 0

## Gaps
