---
phase: 01-environment-oracle-foundation
plan: 3
subsystem: infra
tags: [docker, airflow, dockerfile, oracle-provider, csv-processor-scaffold]

# Dependency graph
requires: ["01-01", "01-02"]
provides:
  - "docker/airflow/Dockerfile — custom Airflow image (D-12) with oracledb, pydantic, apache-airflow-providers-standard, apache-airflow-providers-oracle, and the local csv_processor scaffold"
  - "docker-compose.yml's 5 Airflow runtime services (airflow-init/apiserver/scheduler/dag-processor/triggerer) build from the custom Dockerfile instead of the stock image"
  - "oracle_default Airflow Connection registered via AIRFLOW_CONN_ORACLE_DEFAULT (D-11), confirmed testable end-to-end"
  - "packages/csv-processor/ empty package scaffold (D-16 repo layout) for Phase 3 to fill in"
affects: ["01-04", "phase-2-config-contract", "phase-3-csv-engine", "phase-4-oracle-bulk-load", "phase-5-dag-wiring"]

# Actuals (#2632)
actuals:
  tokens: 820
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: ["apache-airflow-providers-oracle==4.6.2"]
  patterns:
    - "Custom Airflow image built from docker/airflow/Dockerfile, never _PIP_ADDITIONAL_REQUIREMENTS — every pinned package resolved against the official Airflow 3.3.1 constraints file"
    - "COPY --chown=airflow:0 + pip install --no-deps for the local csv_processor package, matching the reference repo's own dataplat pattern"

key-files:
  created:
    - docker/airflow/Dockerfile
    - packages/csv-processor/pyproject.toml
    - packages/csv-processor/src/csv_processor/__init__.py
  modified:
    - docker-compose.yml

key-decisions:
  - "apache-airflow-providers-standard version corrected from the plan's 1.18.0 to 1.17.0 — the official Airflow 3.3.1 constraints file (constraints-3.12.txt) pins 1.17.0, and 1.18.0 conflicted with it, breaking the whole point of using --constraint for deterministic installs"
  - "apache-airflow-providers-oracle==4.6.2 added (not in the original plan) — required for Airflow to have a registered Hook for conn_type \"oracle\"; without it `airflow connections test` fails with \"Unknown hook type\" regardless of the Connection URI being valid. Approved via checkpoint:human-verify (package-legitimacy gate) with the user explicitly confirming Oracle support is a large project priority."
  - "AIRFLOW__CORE__TEST_CONNECTION set to \"Enabled\" — Airflow 3 defaults this to \"Disabled\" as a security default (test-connection can probe internal network reachability); enabled here since this project's local-dev-only scope explicitly needs oracle_default to be testable (D-11)."

requirements-completed: [INFRA-01, INFRA-03]

coverage:
  - id: D1
    description: "docker/airflow/Dockerfile builds FROM apache/airflow:3.3.1-python3.12, installing oracledb==4.0.2, pydantic==2.13.4, apache-airflow-providers-standard==1.17.0, apache-airflow-providers-oracle==4.6.2 (pinned + constrained), and the local csv_processor scaffold with --no-deps"
    requirement: INFRA-01
    verification:
      - kind: integration
        ref: "docker compose build (all 5 Airflow services built with no errors)"
        status: pass
      - kind: integration
        ref: "docker compose exec airflow-scheduler python -c \"import csv_processor; import oracledb; import pydantic\" (exit 0)"
        status: pass
    human_judgment: false
  - id: D2
    description: "docker-compose.yml's Airflow runtime services build from the custom Dockerfile and the full stack boots healthy"
    requirement: INFRA-01
    verification:
      - kind: integration
        ref: "docker compose up -d --wait (all 6 services healthy/running)"
        status: pass
    human_judgment: false
  - id: D3
    description: "oracle_default Connection registered via AIRFLOW_CONN_ORACLE_DEFAULT is visible and testable in Airflow, independent of csv_processor"
    requirement: INFRA-03
    verification:
      - kind: integration
        ref: "airflow connections get oracle_default (exits 0, extra_dejson shows service_name=FREEPDB1 parsed correctly)"
        status: pass
      - kind: integration
        ref: "airflow connections test oracle_default (\"Connection success!\")"
        status: pass
    human_judgment: false
  - id: D4
    description: "No regression in prior verification after the custom-image swap"
    verification:
      - kind: integration
        ref: "uv run python scripts/verify_environment.py (all 3 OK lines, exit 0)"
        status: pass
    human_judgment: false
  - id: D5
    description: "New package (apache-airflow-providers-oracle, not in original plan) reviewed for legitimacy before install"
    verification: []
    human_judgment: true
    rationale: "Human explicitly typed 'approved' after reviewing PyPI/GitHub evidence (122 releases, same apache/airflow monorepo/namespace as the already-approved apache-airflow-providers-standard) presented in the checkpoint — a judgment call, not an automatable check."

# Metrics
duration: 13min
completed: 2026-08-28
status: complete
---

# Phase 1 Plan 3: Custom Airflow Image + csv-processor/dags Scaffolds Summary

**The stock `apache/airflow:3.3.1-python3.12` image is replaced by a custom-built image (D-12) that pip-installs this project's pinned dependencies (including `apache-airflow-providers-oracle`, discovered as required mid-execution) and the local `csv_processor` package scaffold; the `oracle_default` Connection is registered and confirmed testable end-to-end.**

## Performance

- **Duration:** ~13 min (excludes checkpoint wait time for the mid-plan package-legitimacy approval)
- **Completed:** 2026-08-28
- **Tasks:** 2/2 completed
- **Files modified:** 4 (3 created, 1 modified across both tasks)

## Accomplishments

- Wrote `docker/airflow/Dockerfile`: `FROM apache/airflow:3.3.1-python3.12`, no `USER root` switch, pinned `pip install` constrained against the official Airflow 3.3.1 constraints file, `COPY --chown=airflow:0` + `pip install --no-deps` for the local `csv_processor` package
- Scaffolded `packages/csv-processor/` (D-16 repo layout) — `pyproject.toml` (hatchling build backend, matching the reference repo's own convention) and an empty `csv_processor/__init__.py` with only `__version__` — no processing logic, Phase 3's scope
- Rewired `docker-compose.yml`'s `x-airflow-common` anchor from a stock `image:` to a `build:` block (`context: .`, `dockerfile: docker/airflow/Dockerfile`), applying to all 5 Airflow runtime services (`airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`)
- Registered `AIRFLOW_CONN_ORACLE_DEFAULT` (D-11) in the shared environment block with the container-internal `oracle` hostname
- Built and booted the full stack from the custom image — all 6 services healthy — and confirmed `csv_processor`/`oracledb`/`pydantic` all import cleanly inside the container
- Confirmed the `oracle_default` Connection resolves (`airflow connections get`) and tests successfully (`airflow connections test`), resolving RESEARCH.md's Open Question 2 (the `?service_name=FREEPDB1` URI form parses correctly)
- Re-ran `scripts/verify_environment.py` from the host — no regression from the image swap

## Task Commits

Each task was committed atomically:

1. **Task 1: Custom Airflow Dockerfile + docker-compose build wiring + csv-processor/dags scaffolds** - `f61edf0` (feat)
2. **Task 2: Verify Oracle Connection registration and re-confirm environment integrity** - `92eb1a3` (feat) — includes the mid-task `apache-airflow-providers-oracle` addition, approved via checkpoint

## Files Created/Modified

- `docker/airflow/Dockerfile` - custom Airflow image; pinned `oracledb==4.0.2`, `pydantic==2.13.4`, `apache-airflow-providers-standard==1.17.0`, `apache-airflow-providers-oracle==4.6.2`, constrained against `constraints-3.3.1/constraints-3.12.txt`; local `csv_processor` package installed with `--no-deps`
- `packages/csv-processor/pyproject.toml` - minimal package metadata (hatchling backend, no dependencies yet)
- `packages/csv-processor/src/csv_processor/__init__.py` - empty scaffold, `__version__ = "0.1.0"` only
- `docker-compose.yml` - `x-airflow-common` anchor now builds from the custom Dockerfile; added `AIRFLOW_CONN_ORACLE_DEFAULT` and `AIRFLOW__CORE__TEST_CONNECTION` to the shared environment block

Note: `airflow/dags/.gitkeep` (also listed in this plan's `files_modified`) already existed from Plan 01-01's tracer — no action needed here.

## Decisions Made

- Corrected `apache-airflow-providers-standard` from the plan's stated `1.18.0` to `1.17.0` — the official Airflow 3.3.1 constraints file pins `1.17.0`; using `1.18.0` produced a `ResolutionImpossible` conflict during `docker compose build`, defeating the entire purpose of pinning a `--constraint` URL for deterministic installs.
- Added `apache-airflow-providers-oracle==4.6.2` (not originally in the plan) after discovering `airflow connections test oracle_default` fails with `Unknown hook type "oracle"` without it — Airflow's connection-test path requires a registered Hook class per conn_type, which only a provider package supplies (the raw `oracledb` driver alone is not enough). Version `4.6.2` matches the same official constraints file already governing the other two provider pins. Presented as a `checkpoint:human-verify` (package-legitimacy gate, `gate="blocking-human"`) per the deviation rules' package-install carve-out; user approved explicitly, noting Oracle support is a large project priority — satisfied here via the fully open-source (Apache-2.0), same-monorepo `apache-airflow-providers-oracle` package, not any proprietary add-on.
- Enabled `AIRFLOW__CORE__TEST_CONNECTION: "Enabled"` — Airflow 3 defaults this to `"Disabled"` as a deliberate security default (the test-connection endpoint can probe internal network reachability); this project's explicit local-dev-only scope (already accepting T-01-05's plaintext-credential disposition) makes enabling it here a reasonable, documented relaxation, consistent with D-11's stated goal of a UI-testable Oracle connection.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `apache-airflow-providers-standard==1.18.0` conflicted with the pinned constraints file**
- **Found during:** Task 1, first `docker compose build` attempt
- **Issue:** `pip` reported `ResolutionImpossible` — the plan's stated version (`1.18.0`) conflicted with the constraints file's own pin (`1.17.0`) for the same package.
- **Fix:** Changed the Dockerfile's pinned version to `1.17.0`, matching the constraints file exactly.
- **Files modified:** `docker/airflow/Dockerfile`
- **Commit:** `f61edf0`

**2. [Rule 3 - Blocking] `airflow connections test` failed with "Unknown hook type" due to a missing provider package — required a checkpoint, not a silent auto-fix**
- **Found during:** Task 2, first `airflow connections test oracle_default` attempt
- **Issue:** The Oracle Connection registered and resolved correctly via the generic `AIRFLOW_CONN_*` mechanism, but Airflow's `connections test` subcommand requires a Hook class registered for the `oracle` conn_type — supplied only by the `apache-airflow-providers-oracle` package, which was not in the plan's install list.
- **Fix:** Per the deviation rules' explicit carve-out (package-manager installs are never auto-fixable, even in auto mode), paused with a `checkpoint:human-verify` (`gate="blocking-human"`) presenting PyPI/GitHub legitimacy evidence for `apache-airflow-providers-oracle==4.6.2`. User approved; added the package, rebuilt, and confirmed `airflow connections test oracle_default` now reports `"Connection success!"`.
- **Files modified:** `docker/airflow/Dockerfile`, `docker-compose.yml` (also added `AIRFLOW__CORE__TEST_CONNECTION: "Enabled"` in the same pass, since `connections test` is disabled by default in Airflow 3)
- **Commit:** `92eb1a3`

## Checkpoints

**checkpoint:human-verify (package-legitimacy, gate=blocking-human)** — mid-Task-2, for `apache-airflow-providers-oracle==4.6.2`. Presented PyPI evidence (122 published releases, same `apache/airflow` GitHub monorepo/namespace as the already-approved `apache-airflow-providers-standard`, Apache-2.0 license). User approved explicitly, noting Oracle support is a stated project priority. No alternative package was substituted — this is the sole, correct, official Oracle provider for Airflow.

## Issues Encountered

- `.env.example` could not be edited via Claude Code's Read/Write/Edit/Bash tools — a global user-level permission deny rule (`Read(.env.*)` in `~/.claude/settings.json`) blocks all tool access to any `.env*`-family file, including the committed, secret-free `.env.example` template. This is a legitimate, intentional security boundary (not something to work around), so the plan's action item "append a comment to `.env.example` noting `AIRFLOW_CONN_ORACLE_DEFAULT` is set directly in docker-compose.yml" was **not completed**. The underlying fact this comment would have documented is still true and already covered by the code comment added directly in `docker-compose.yml` (see the `AIRFLOW_CONN_ORACLE_DEFAULT` line's inline comment). No functional impact — this was documentation-only, and the equivalent information now lives in `docker-compose.yml` itself, arguably a better location since it's adjacent to the actual env var.

## User Setup Required

None — no external service configuration required beyond what Plan 01-01/01-02 already established (docker-compose stack running locally). Existing developers with a stale image cache should run `docker compose build && docker compose up -d --wait` to pick up the custom image (already done in this session).

## Known Stubs

None. `packages/csv-processor/src/csv_processor/__init__.py` is an intentional empty scaffold — Phase 3's scope, not a stub masking incomplete work in this phase.

## Known Gaps

1. **`.env.example` comment not added** (see Issues Encountered above) — the permission boundary blocking this is a global user-level setting, not project-specific; a human with direct filesystem access (outside Claude Code's tool restrictions) could add the one-line comment if desired, but it's not functionally required — `docker-compose.yml`'s own inline comment already documents the same fact.

## Next Phase Readiness

The custom Airflow image is proven end-to-end: build succeeds, all 6 services boot healthy from it, `csv_processor`/`oracledb`/`pydantic` import cleanly inside the container, and `oracle_default` is both registered and testable. `packages/csv-processor/` and `airflow/dags/` scaffolds exist for Phase 3 and Phase 5 respectively. Plan 01-04 (Makefile + `docs/environment.md`) can now document the full lifecycle against this custom-image-based stack, including the two mid-plan additions (`apache-airflow-providers-oracle`, `AIRFLOW__CORE__TEST_CONNECTION`) as part of the finished environment picture. No blockers.

---
*Phase: 01-environment-oracle-foundation*
*Completed: 2026-08-28*

## Self-Check: PASSED
