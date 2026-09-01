# Phase 8: Environment & Docker Fixes for Container-Side Generation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-09-01
**Phase:** 8-Environment & Docker Fixes for Container-Side Generation
**Areas discussed:** docs/environment.md rewrite, Bundle the passwords-file fix too?, Permanent automated verification?, Passwords-file fix mechanism, Verification check architecture, Write-check rigor

---

## docs/environment.md rewrite

| Option | Description | Selected |
|--------|-------------|----------|
| Replace with a note that it's automatic now | Remove the manual mkdir step and the gotcha section, replace with a short line noting airflow-init now chowns data/ automatically | ✓ |
| Leave the doc untouched this phase | Treat docs/environment.md as out of scope for Phase 8 | |

**User's choice:** Replace with a note that it's automatic now (Recommended)
**Notes:** Follow-up questions resolved tone and additional-content scope:

| Follow-up: Doc tone | Description | Selected |
|--------|-------------|----------|
| Brief historical note | One line noting what changed and why | |
| Clean current-state only | Just describe how it works today, no mention of the prior workaround | ✓ |

**User's choice:** Clean current-state only

| Follow-up: New capability | Description | Selected |
|--------|-------------|----------|
| Document it briefly | A short note that the container can now run the generator in-process | ✓ |
| Skip — implementation detail only | Leave it undocumented | |

**User's choice:** Document it briefly (Recommended)

---

## Bundle the passwords-file fix too?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, bundle it in Phase 8 | Same root-cause class, same phase already touching airflow-init/compose | ✓ |
| No, stay strictly scoped to ENV-01/ENV-02 | Leave the passwords-file gotcha as a documented manual step | |

**User's choice:** Yes, bundle it in Phase 8 (Recommended)
**Notes:** Follow-up resolved the exact mechanism:

| Follow-up: Passwords-file fix mechanism | Description | Selected |
|--------|-------------|----------|
| Same airflow-init compose-level pattern | airflow-init also ensures the passwords file exists with correct content/perms before other services start | ✓ |
| Makefile-level pre-flight step | `make up` itself checks/creates the file on the host before calling docker compose up | |

**User's choice:** Same airflow-init compose-level pattern (Recommended)

---

## Permanent automated verification?

| Option | Description | Selected |
|--------|-------------|----------|
| Add a permanent check | Extend scripts/verify_environment.py with an import+write-access assertion, wired into verify-phase8 | ✓ |
| One-off manual check only | Verify during execution but don't add permanent code | |

**User's choice:** Add a permanent check (Recommended)
**Notes:** Two follow-ups resolved architecture and rigor:

| Follow-up: Verification check architecture | Description | Selected |
|--------|-------------|----------|
| Add it into verify_environment.py | Extend the existing canonical script with a docker-exec-based check | ✓ |
| Separate Makefile-only check | Keep verify_environment.py purely network-based; add the check as its own Makefile recipe | |

**User's choice:** Add it into verify_environment.py (Recommended)

| Follow-up: Write-check rigor | Description | Selected |
|--------|-------------|----------|
| Write-then-delete a real test file | Stronger proof, matches project's "verify by actually doing it" habit | ✓ |
| Check permission bits/ownership only | Simpler, no cleanup needed | |

**User's choice:** Write-then-delete a real test file (Recommended)

---

## Claude's Discretion

- Exact REQ-ID handling for the bundled passwords-file fix (fold into ENV-02 vs. new ENV-03) —
  planner's call during planning, following REQUIREMENTS.md's existing ID convention.
- Exact recursion/idempotency shape of the `airflow-init` chown step.
- Exact probe-file naming/location for the write-then-delete check (must not match
  `customers_*.csv*`/`orders_*.csv*` glob patterns).
- Exact wording/placement of the new-capability doc note in `docs/environment.md`.

## Deferred Ideas

None — discussion stayed within Phase 8's domain. The passwords-file bundling is a scope
expansion within Phase 8 (same root-cause class as ENV-02), not a new capability belonging to a
different phase.
