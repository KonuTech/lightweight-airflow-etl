---
phase: 08-environment-docker-fixes-for-container-side-generation
plan: 02
subsystem: infra
tags: [docker-compose, verify-environment, makefile, bind-mounts, simple-auth-manager]

# Dependency graph
requires:
  - phase: 08-environment-docker-fixes-for-container-side-generation (plan 01)
    provides: generator/ mount, extended PYTHONPATH, faker==40.37.0, airflow-init repair step (initial version)
provides:
  - "verify_generator_importable()/verify_data_write_access() permanent regression checks in scripts/verify_environment.py"
  - "make verify-phase8 Makefile target"
  - "corrected passwords-file bind-mount fix (docker/airflow/secrets/ directory mount, replacing the broken single-file rmdir approach)"
  - "clean current-state docs/environment.md (Generator Container Mount section, obsolete gotchas removed)"
affects: [phase-09-csv-generate-schedule-dag]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "docker compose exec -T subprocess-based container verification, narrow retry-on-container-not-ready only (mirrors verify_airflow_auth's transient-retry discipline)"
    - "mount the parent directory (git-tracked via .gitkeep) instead of a single file when a bind-mounted path must always exist as a real file -- avoids Docker's auto-create-as-directory bug turning the target into a busy, un-rmdir-able mountpoint"

key-files:
  created: [docker/airflow/secrets/.gitkeep]
  modified:
    - scripts/verify_environment.py
    - Makefile
    - docs/environment.md
    - docker-compose.yml
    - .gitignore

key-decisions:
  - "Plan 08-01's airflow-init rmdir-the-auto-created-directory fix does not work: live-reproduced during this plan's own genuine fresh-clone proof (rm -f the passwords file + make destroy + make up), rmdir fails with 'Device or resource busy' because the auto-created directory is itself a live bind-mount target inside airflow-init's own container -- a busy mountpoint can never be rmdir'd from inside the very container it's mounted into"
  - "Fixed by mounting the PARENT directory (docker/airflow/secrets/, tracked via .gitkeep) instead of the passwords file directly -- a missing directory auto-creates fine as a directory (the desired type), and the file living inside it is a plain non-mountpoint file airflow-init can freely create/chown/chmod. Pointed SimpleAuthManager at the new path via AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"
  - "airflow-apiserver chosen as the docker compose exec target for the new container-capability checks, consistent with 08-RESEARCH.md's discretionary call (verify-phase5's existing Makefile check already uses airflow-scheduler; either works, mounts/PYTHONPATH/image are identical)"

patterns-established:
  - "verify-phaseN targets that introduce no new pytest-testable logic are a thin one-line wrapper around scripts/verify_environment.py (matches verify-phase8's own comment)"

requirements-completed: [ENV-01, ENV-02, ENV-03]

# Metrics
duration: 25min
completed: 2026-09-01
---

# Phase 8 Plan 02: Environment & Docker Fixes for Container-Side Generation Summary

Added permanent `verify_generator_importable()`/`verify_data_write_access()` container-exec checks
plus a `make verify-phase8` target, and — while performing this plan's own literal genuine
fresh-clone proof — discovered and fixed a real bug in Plan 08-01's passwords-file repair mechanism
(rmdir-ing a busy bind-mount from inside the same container it's mounted into never works), then
rewrote `docs/environment.md` to present clean current-state documentation with the manual
passwords-file step now genuinely eliminated (not just proactively repaired).

## Performance

- **Duration:** ~25 min
- **Started:** 2026-09-01T17:05:00Z (approx.)
- **Completed:** 2026-09-01T17:24:00Z
- **Tasks:** 3 completed
- **Files modified:** 5 (`scripts/verify_environment.py`, `Makefile`, `docs/environment.md`,
  `docker-compose.yml`, `.gitignore`) + 1 created (`docker/airflow/secrets/.gitkeep`)

## Accomplishments

- Permanent, committed regression check (`verify_generator_importable()`/
  `verify_data_write_access()`, wired into `scripts/verify_environment.py`'s `main()`) proving
  ENV-01/ENV-02 continuously, catching a future regression (e.g. someone removing the `generator/`
  mount) automatically — D-06/D-07/D-08.
- `make verify-phase8` added, following the `verify-phase2..7` convention exactly.
- A genuinely fresh-clone boot (real `rm -f` the passwords file + `make destroy` (removes
  volumes/images) + `rm -rf data/` + `make up`) now succeeds with zero `PermissionError`/
  `IsADirectoryError` in any service's logs, and a second `make up` against the now-initialized
  state also succeeds (idempotency) — ROADMAP.md Success Criteria 2 and 3 proven for real, not by
  code inspection.
- `docs/environment.md` rewritten: new "Generator Container Mount" section; both obsolete
  "Known First-Boot Gotcha" sections removed; "First-Clone Setup Gaps" now lists only the
  untouched `.env` step, since the passwords-file manual-create step is now genuinely unnecessary
  (not just proactively repaired).

## Task Commits

Each task was committed atomically:

1. **Task 1: verify_environment.py — container-exec import + write-access checks** - `3b9943c` (feat)
2. **Task 2: Makefile verify-phase8 target + real fresh-clone and idempotency proof (+ Rule 1/3
   bug fix to Plan 08-01's passwords-file mechanism)** - `a8de5a2` (feat)
3. **Task 3: docs/environment.md — clean current-state rewrite** - `c806982` (docs)

## Files Created/Modified

- `scripts/verify_environment.py` — added `_docker_compose_exec()`, `verify_generator_importable()`,
  `verify_data_write_access()`, wired into `main()` after `verify_airflow_auth()`.
- `Makefile` — added `verify-phase8` target (`.PHONY` + thin wrapper around
  `verify_environment.py`).
- `docker-compose.yml` — mount `./docker/airflow/secrets:/opt/airflow/secrets` (directory) instead
  of the single passwords file; added `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` env var;
  simplified `airflow-init`'s repair command (dropped the now-ineffective `rmdir`-if-directory
  step, since the root cause it targeted no longer applies).
- `docker/airflow/secrets/.gitkeep` (new) — guarantees the mount source directory always exists on
  a fresh clone, so Docker never needs to auto-create it (and therefore never picks the wrong
  type).
- `.gitignore` — updated the ignored passwords-file path to the new `docker/airflow/secrets/`
  location.
- `docs/environment.md` — "Generator Container Mount" section added; both obsolete gotcha sections
  and the manual passwords-file creation step removed.

## Decisions Made

- **Rule 1/3 auto-fix — Plan 08-01's passwords-file repair mechanism was broken for the actual
  bug it targeted.** Live-reproduced during this plan's own mandated fresh-clone proof: deleting
  the passwords file, then `make destroy && make up`, made Docker auto-create the host path as a
  directory (as expected) and bind-mount it into every Airflow container including `airflow-init`
  — but `airflow-init`'s own `rmdir` of that path then failed with `Device or resource busy`,
  because the directory is itself a live mountpoint inside `airflow-init`'s own container; a busy
  mountpoint cannot be `rmdir`'d from inside the very container it's mounted into. This is not a
  scope expansion — it's the exact D-04/D-05 bundled fix this phase already committed to, just
  correcting a mechanism that turned out not to work when actually exercised end-to-end (Plan
  08-01's own summary explicitly flagged this exact end-to-end reproduction as "Not Performed" in
  its own checkout, to avoid destroying dev-only Oracle/Postgres state — this plan's Task 2 was
  the first time it was actually exercised).
- **Fix mechanism:** mount the parent directory (`docker/airflow/secrets/`, tracked via
  `.gitkeep`) instead of the single file. A missing directory auto-creates fine as a directory
  (the desired type for a directory mount), sidestepping the whole bug class; the passwords file
  living inside it is then a plain, non-mountpoint file that `airflow-init` can freely
  create/chown/chmod without ever hitting EBUSY.
- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` set explicitly to the new path (default
  falls back to `AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`, which no longer
  matches the new mount location).
- `airflow-apiserver` used as the `docker compose exec` target for the new checks (RESEARCH.md's
  A2 discretionary call — equivalent to `airflow-scheduler`, which `verify-phase5` already uses).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1/3 - Bug / Blocking] Passwords-file bind-mount repair mechanism from Plan 08-01 does
not actually work against a genuine fresh-clone reproduction**
- **Found during:** Task 2's own mandated live proof (`rm -f
  docker/airflow/simple_auth_manager_passwords.json.generated && make destroy && make up`)
- **Issue:** `airflow-init`'s `rmdir "$PWFILE"` step (Plan 08-01) fails with `Device or resource
  busy` because the Docker-auto-created directory is itself a live bind-mount target inside
  `airflow-init`'s own container — you cannot `rmdir` a busy mountpoint from inside the very
  container it's mounted into. This left `airflow-apiserver` crashing with
  `IsADirectoryError: [Errno 21] Is a directory:
  '/opt/airflow/simple_auth_manager_passwords.json.generated'` on a genuine fresh boot, exactly
  the class of bug D-04/D-05 was supposed to eliminate.
- **Fix:** Changed the bind mount from a single file to its parent directory
  (`./docker/airflow/secrets:/opt/airflow/secrets`, git-tracked via `.gitkeep` so it always
  exists), pointed `SimpleAuthManager` at the new location via
  `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`, and simplified `airflow-init`'s command
  (dropped the now-unnecessary `rmdir` step).
- **Files modified:** `docker-compose.yml`, `.gitignore`, `docker/airflow/secrets/.gitkeep` (new)
- **Verification:** Real `rm -f` the old file + `rm -rf data/` + `make destroy` (removes
  volumes/images) + `make up` — all 6 services report healthy, zero `PermissionError`/
  `IsADirectoryError` in any log; `make verify-phase8` exits 0; a second `make up` against the
  already-initialized state also exits 0.
- **Committed in:** `a8de5a2` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1/3 — bug in a prior plan's fix mechanism, discovered and
corrected while performing this plan's own mandated live-verification steps).
**Impact on plan:** Essential — without this fix, Success Criteria 2/3 (a genuinely fresh clone
booting cleanly, idempotent `make up`) would have failed for real, not just in code review. No
scope creep: the fix stays within the exact same D-04/D-05 bundled-fix territory this phase
already owns; `README.md`/`docs/development.md` still reference the old manual
`echo '{"admin": "admin"}' > docker/airflow/simple_auth_manager_passwords.json.generated` snippet
(now describing an unused path since the compose mount no longer targets it) — out of this plan's
declared file scope (`docs/environment.md` only per D-01/D-02/D-03), left as a documentation
cleanup for a follow-up pass rather than expanded into here. Also out of scope and left untouched:
`.github/workflows/ci.yml`/`readme-summary.yml`'s pre-create steps for the old passwords-file path
become redundant (not broken — `airflow-init` now seeds the file itself either way) but were not
removed, matching 08-RESEARCH.md's own "Open Question 1" precedent of leaving CI config changes to
planner/executor discretion outside this plan's literal locked scope.

## Issues Encountered

None beyond the auto-fixed issue documented above — Task 1 (verify_environment.py extension) and
Task 3 (docs rewrite) both matched their acceptance criteria on the first pass.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- ENV-01/ENV-02/ENV-03 are now genuinely, permanently verified: `make verify-phase8` proves
  container-side generator importability and real data-write access on every run, and the
  underlying compose mechanism has been proven against an actual from-scratch boot (not just
  reasoned about).
- Phase 9 (`csv_generate_schedule` DAG) can rely on `/opt/airflow/generator` +
  `from generator.generate_csv import main` + writable `data/customers/`/`data/orders/` being
  present on any developer's or CI's fresh clone with zero manual setup.
- Deferred cleanup for a future pass (not blocking Phase 9): `README.md`, `docs/development.md`,
  and `.github/workflows/ci.yml`/`readme-summary.yml` still reference the old, now-unused
  `docker/airflow/simple_auth_manager_passwords.json.generated` single-file path in their
  first-clone/CI-bootstrap instructions.

## Self-Check: PASSED

- `scripts/verify_environment.py` — FOUND, contains `def verify_generator_importable`,
  `def verify_data_write_access`, `def _docker_compose_exec` (each exactly once).
- `Makefile` — FOUND, contains `verify-phase8:` target and `verify-phase8` in `.PHONY`.
- `docs/environment.md` — FOUND, contains `## Generator Container Mount`; no longer contains
  `mkdir -p docker/airflow` or either obsolete gotcha heading.
- `docker-compose.yml` — FOUND, contains `./docker/airflow/secrets:/opt/airflow/secrets` and
  `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`.
- `docker/airflow/secrets/.gitkeep` — FOUND.
- Commit `3b9943c` — FOUND in `git log --oneline`.
- Commit `a8de5a2` — FOUND in `git log --oneline`.
- Commit `c806982` — FOUND in `git log --oneline`.
- Live verification re-confirmed at time of writing: `uv run python scripts/verify_environment.py`
  and `make verify-phase8` both exit 0 against the currently running (genuinely fresh-booted)
  stack.

---
*Phase: 08-environment-docker-fixes-for-container-side-generation*
*Completed: 2026-09-01*
