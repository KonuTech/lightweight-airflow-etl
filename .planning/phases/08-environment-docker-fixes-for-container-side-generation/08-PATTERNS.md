# Phase 8: Environment & Docker Fixes for Container-Side Generation - Pattern Map

**Mapped:** 2026-09-01
**Files analyzed:** 5 (all modified, none newly created — this phase is wiring-only per RESEARCH.md
"Recommended Project Structure")
**Analogs found:** 5 / 5 (every file is edited in place; the "analog" for each is the file's own
existing conventions plus one cross-file precedent)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|--------------------|------|-----------|-----------------|---------------|
| `docker-compose.yml` (edit: `x-airflow-common` mount/`PYTHONPATH`, `airflow-init` command) | config | event-driven (container startup/orchestration sequencing) | same file's own `x-airflow-common` anchor + `airflow-init` service (self-analog); secondary: Apache Airflow's own quick-start `docker-compose.yaml` `airflow-init` root-chown pattern (cited in RESEARCH.md, not in this repo) | exact (self) |
| `docker/airflow/Dockerfile` (edit: add `faker==40.37.0` to constrained `pip install`) | config | batch (build-time dependency install) | same file's own existing constrained `pip install` stanza (self-analog) | exact (self) |
| `scripts/verify_environment.py` (edit: add `verify_generator_importable()`, `verify_data_write_access()`, `_docker_compose_exec()` helper, wire into `main()`) | utility / test | request-response (subprocess exec) + file-I/O (write-then-delete probe) | `verify_airflow_auth()` in the same file (retry/backoff-on-transient-failure convention); `verify_tables()`/`verify_columns()` (standalone-function, assert-and-raise convention) | exact (self, same file's own conventions) |
| `Makefile` (edit: add `verify-phase8` target) | config | batch (test/verification orchestration) | `verify-phase5`/`verify-phase6`/`verify-phase7` targets in the same file | exact |
| `docs/environment.md` (edit: rewrite "First-Clone Setup Gaps" + both "Known First-Boot Gotcha" sections per D-01/D-02/D-03) | config (docs) | transform (content rewrite) | same file's own existing "First-Clone Setup Gaps" / "Known First-Boot Gotcha" / "Verifying the Stack" sections (self-analog) | exact (self) |

**No new files are created by this phase** — RESEARCH.md's "Recommended Project Structure" section
is explicit: "No new files/directories — this phase only edits existing files." There is no
"No Analog Found" table below for this reason; every touched file already exists and its own
pre-existing conventions are the primary analog, cross-checked against one or two sibling
precedents in the same file (e.g. `verify-phase5..7` for the new `verify-phase8`).

## Pattern Assignments

### `docker-compose.yml` (config, event-driven orchestration)

**Analog:** the file's own `x-airflow-common` anchor (lines 1-126) and `airflow-init` service
(lines 160-167), read in full above.

**Existing volumes block to extend** (lines 94-120):
```yaml
  volumes:
    - ./docker/airflow/simple_auth_manager_passwords.json.generated:/opt/airflow/simple_auth_manager_passwords.json.generated
    - ./airflow/dags:/opt/airflow/dags
    - ./data:/opt/airflow/data
    - ./configs:/opt/airflow/configs:ro
    - airflow-logs:/opt/airflow/logs
```
Add a new `./generator:/opt/airflow/generator:ro` line, following the exact same
`./hostpath:/opt/airflow/containerpath[:ro]` shape already used for `./configs`.

**Existing PYTHONPATH env var to extend** (line 92):
```yaml
    PYTHONPATH: "/opt/airflow/dags"
```
Becomes `"/opt/airflow/dags:/opt/airflow"` — follow the file's own documented convention of a
comment block directly above each non-obvious env var explaining *why* (see the
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL`/`AIRFLOW__API_AUTH__JWT_SECRET` comments at lines 25-56
for the house style: multi-line comment citing the discovering phase/session, the concrete failure
mode observed, and the fix rationale).

**Existing `airflow-init` service to extend** (lines 160-167):
```yaml
  airflow-init:
    <<: *airflow-common
    depends_on:
      postgres:
        condition: service_healthy
    command: db migrate
    environment:
      <<: *airflow-common-env
```
Add `user: "0:0"` (root override, this service only — every other service inherits
`x-airflow-common`'s non-root `user: "${AIRFLOW_UID:-50000}:0"` unchanged at line 93) and replace
`command: db migrate` with the combined `bash -c "..."` block from RESEARCH.md's "Pattern 1"/"Full
docker-compose.yml diff shape" section (repairs passwords file, then `mkdir -p`/`chown -R data/`,
then `exec airflow db migrate`). This is the single load-bearing pattern for D-05's compose-level
fix mechanism — copy RESEARCH.md's exact shell logic (rmdir-if-directory, seed-if-missing, chown,
chmod, mkdir -p, chown -R, exec db migrate), not a paraphrase, matching this project's own
established "copy verbatim, not paraphrased" discipline already applied once in
`.github/workflows/ci.yml` (see Shared Patterns below).

---

### `docker/airflow/Dockerfile` (config, batch build-time install)

**Analog:** the file's own existing constrained `pip install` stanza (lines 11-20, full file
already read above — only 24 lines total, no further reads needed).

**Exact insertion point:**
```dockerfile
RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.17.0" \
      "apache-airflow-providers-oracle==4.6.2" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
 && pip install --no-cache-dir \
      "clevercsv==0.8.5" \
      "charset-normalizer==3.5.1" \
      "chardet==7.6.0"
```
Add `"faker==40.37.0" \` as a new line inside the **first** (constrained) `pip install` call, right
after `"apache-airflow-providers-oracle==4.6.2" \` — NOT the second unconstrained call, which is
reserved (per the file's own comment at lines 3-10) for `csv_processor`'s own detection-library
deps that Airflow's constraints file never touches. RESEARCH.md confirms `faker` has zero entry in
Airflow's `constraints-3.3.1/constraints-3.12.txt`, so the constrained line is safe.

---

### `scripts/verify_environment.py` (utility/test, request-response + file-I/O)

**Analog:** `verify_airflow_auth()` (lines 117-172) for the retry/backoff-on-transient-failure
shape; `verify_tables()`/`verify_columns()` (lines 52-83) for the standalone-function,
assert-and-raise, docstring convention.

**Imports pattern** (lines 16-24 — module already uses `from __future__ import annotations` and a
`TYPE_CHECKING`-gated heavy import; the new `subprocess`/`time` imports are already-present stdlib
modules, `time` already imported at line 21):
```python
from __future__ import annotations

import http.client
import json
import sys
import time
import urllib.error
import urllib.request
from typing import TYPE_CHECKING
```
Add `import subprocess` alongside these — no new third-party dependency, matches the file's
existing "stdlib only in the module-level import block" discipline (`oracledb` stays
`TYPE_CHECKING`-gated / lazily imported inside `main()`, per the WR-03 comment at lines 27-31 and
line 176 — the new container-exec check needs no such gating since `subprocess` is always
available).

**Retry/backoff pattern to mirror** (lines 140-171, `verify_airflow_auth`'s loop):
```python
    for attempt in range(1, AUTH_RETRY_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = json.loads(response.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as exc:
            raise AssertionError(
                f"Airflow auth request failed with HTTP {exc.code}: {exc.read().decode('utf-8')}"
            ) from exc
        except (
            urllib.error.URLError,
            OSError,
            http.client.IncompleteRead,
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as exc:
            if attempt < AUTH_RETRY_ATTEMPTS:
                delay = min(
                    AUTH_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                    AUTH_RETRY_MAX_DELAY_SECONDS,
                )
                print(f"... retrying in {delay}s ...", file=sys.stderr)
                time.sleep(delay)
                continue
            raise AssertionError(f"... failed after {AUTH_RETRY_ATTEMPTS} attempts: {exc}") from exc
    assert "access_token" in body, f"Response missing access_token field: {body}"
```
Key convention to preserve: **narrow the retry to genuinely transient conditions only**, and let a
"real" failure (here: `urllib.error.HTTPError`, i.e. the server responded but rejected the
request) raise immediately without retrying — RESEARCH.md's "Pattern 2" / Anti-Patterns section
explicitly calls out the same discipline for the new `_docker_compose_exec()` helper (retry only on
"container not ready yet," never on a real `AssertionError`/`ImportError` from the exec'd code).
Use `RESEARCH.md`'s own concrete `_docker_compose_exec()`/`verify_generator_importable()`/
`verify_data_write_access()` code block (already fully drafted in 08-RESEARCH.md's "Recommended
subprocess pattern" section) as the starting implementation — it already follows this file's
constant-naming convention (`CONTAINER_EXEC_TIMEOUT_SECONDS`, `CONTAINER_EXEC_RETRY_ATTEMPTS`,
`CONTAINER_EXEC_RETRY_BASE_DELAY_SECONDS`, mirroring `AUTH_RETRY_ATTEMPTS`/
`AUTH_RETRY_BASE_DELAY_SECONDS`/`AUTH_RETRY_MAX_DELAY_SECONDS` at lines 47-49).

**Assert-and-name-the-culprit docstring convention** (lines 52-58, `verify_tables`):
```python
def verify_tables(cursor: oracledb.Cursor, expected: set[str]) -> None:
    """Assert that every table name in `expected` exists in the current schema's
    USER_TABLES view.

    Raises AssertionError naming any tables missing from `expected` if the query's
    result set doesn't fully cover it. Reusable by Phase 4's Oracle integration tests.
    """
```
New functions (`verify_generator_importable`, `verify_data_write_access`) should follow this exact
docstring shape: one-line summary, a "Raises AssertionError ..." paragraph naming what triggers it,
and a citation of the REQ-ID/decision driving the check (ENV-01/D-06 for importability, ENV-02/D-08
for write access — see RESEARCH.md's own drafted docstrings, which already do this).

**`main()` wiring pattern** (lines 175-234 — sequential calls, each followed by an `OK:` print):
```python
def main() -> int:
    import oracledb  # WR-03: lazy import -- only main() needs the Oracle driver.

    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    try:
        cursor = conn.cursor()
        verify_tables(cursor, expected={...})
        print("OK: all 5 tables exist in ADMIN schema of FREEPDB1")
        ...
    finally:
        conn.close()

    verify_airflow_auth()
    print("OK: admin/admin authenticates against Airflow's /auth/token endpoint")

    return 0
```
Append the two new checks after `verify_airflow_auth()`, each followed by its own `print("OK: ...")`
line — matches the file's `OK:`/`FAILED:` output convention (the `FAILED:` half is already handled
generically by the `__main__` block at lines 238-242, which catches any `AssertionError` from
`main()` — no per-check try/except needed).

**Bottom-of-file error surface** (lines 237-242, unchanged, no edit needed — confirms the new
checks' `AssertionError`s are already caught generically):
```python
if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
```

---

### `Makefile` (config, batch verification orchestration)

**Analog:** `verify-phase5` (lines 58-75) and `verify-phase7` (lines 100-110).

**`verify-phase5` (closest shape — unit suite + a single `docker compose exec -T` live check):**
```makefile
verify-phase5:     ## Phase 5's own combined local gate: unit suite + live DagBag structure check (requires `make up` first)
	uv run pytest tests/unit/ -x
	docker compose exec -T airflow-scheduler python -c "\
from pathlib import Path; \
...
print('DAGBAG_OK')"
```

**`verify-phase7` (most recent target, shows the accreted full shape as later phases added more
steps):**
```makefile
verify-phase7:     ## Phase 7's own combined local gate: unit + e2e + integration suites, lint, and evidence verification (requires `make up` first)
	uv run pytest tests/unit/ -x
	uv run pytest tests/e2e/ -x
	uv run pytest tests/integration/ -x
	$(MAKE) lint
	$(MAKE) verify-evidence
```

RESEARCH.md's own "Code Examples" section already drafts the exact `verify-phase8` shape (no new
unit-testable logic this phase introduces, so it's a thin wrapper around the extended
`scripts/verify_environment.py`, matching the `verify:` target's existing one-liner at line 22-23):
```makefile
verify-phase8:     ## Phase 8's own combined local gate: fresh-clone chown/mount fixes + container capability checks (requires `make up` first)
	uv run python scripts/verify_environment.py
```
Also add `verify-phase8` to the `.PHONY:` line (line 1) alongside `verify-phase6 verify-phase7` —
every prior `verify-phaseN` target is listed there; don't forget this file-wide declaration.

---

### `docs/environment.md` (docs, content rewrite)

**Analog:** the file's own "First-Clone Setup Gaps" (lines 138-155), "Known First-Boot Gotcha:
Permission Error on the Passwords File" (lines 157-167), and "Known First-Boot Gotcha: Permission
Error Creating `data/<dataset>/`" (lines 169-181) sections, all read in full above.

**Section to delete entirely per D-01** ("Known First-Boot Gotcha: Permission Error Creating
`data/<dataset>/`", lines 169-181) — becomes obsolete once the `airflow-init` chown fix lands; no
replacement text, just removal (D-02: no "this used to require X" breadcrumb).

**"First-Clone Setup Gaps" step 2 to delete per D-01** (lines 145-155, the
`mkdir -p docker/airflow && echo '{"admin": "admin"}' > ...` manual block) — becomes obsolete once
D-04/D-05's `airflow-init` passwords-file repair lands. Keep step 1 (`.env`, lines 143-144)
untouched — explicitly out of scope per CONTEXT.md ("the `.env` step is untouched, unrelated").

**"Known First-Boot Gotcha: Permission Error on the Passwords File" (lines 157-167) — also becomes
obsolete** once D-04/D-05 lands (the `airflow-init` repair runs proactively before any service can
hit this crash) — delete, same D-01/D-02 treatment as the `data/` gotcha section.

**New content to add per D-03** — briefly document the new container capability (no existing
section is a direct analog for *this* net-new content since it's describing a capability that
didn't exist before this phase; closest structural sibling is the existing terse, declarative style
of "Port Bindings" (lines 114-136) or "Verifying the Stack" (lines 183-195) — short prose + a fenced
code/yaml snippet, no long narrative). Suggested shape, following that terse-section convention:
```markdown
## Generator Container Mount

`./generator` is mounted read-only at `/opt/airflow/generator`, and `PYTHONPATH` is extended to
include `/opt/airflow` (alongside the existing `/opt/airflow/dags`) so Airflow tasks can
`from generator.generate_csv import main` in-process, without shelling out to a subprocess. `faker`
(the only non-stdlib dependency `generate_csv.py` needs beyond `csv_processor.config`, already
installed) is installed in the image at the exact version pinned in the root `uv.lock`.
```

**"Verifying the Stack" section to extend** (lines 183-195) — the existing three-command sequence
(`docker compose ps`, `scripts/verify_environment.py`, a `curl` auth check) needs no structural
change; `scripts/verify_environment.py`'s own extended output (new `OK:` lines from
`verify_generator_importable`/`verify_data_write_access`) is picked up automatically by the
existing `uv run python scripts/verify_environment.py` line — no doc edit strictly required here
beyond ensuring the prose above it still reads correctly after the gotcha-section deletions.

---

## Shared Patterns

### "Copy verbatim, not paraphrased" for bootstrap/fix shell logic
**Source:** `.github/workflows/ci.yml` lines 48-53 (comment explicitly cites
"docs/environment.md ... copied verbatim, not paraphrased, per 06-RESEARCH.md Pattern 3/Pitfall 7")
**Apply to:** `docker-compose.yml`'s new `airflow-init` `command:` block — copy RESEARCH.md's own
drafted shell logic (Pattern 1 / "Full docker-compose.yml diff shape") exactly, including its
`rmdir`-only-on-empty-directory safety property and the `[ ! -f "$PWFILE" ]`-guarded seed, rather
than re-deriving a similar-but-subtly-different script. This project has already been burned once
by drift between documented and actual bootstrap commands (the CI workflow's own comment
cross-references the docs file specifically to avoid that).

### "Don't trust exit codes — confirm by actually doing the real thing"
**Source:** PROJECT.md working preference (cited in CONTEXT.md D-08 and RESEARCH.md); concretely
already embodied in `scripts/verify_environment.py`'s `verify_tables`/`verify_columns` (query
`USER_TABLES`/`ALL_TAB_COLUMNS` rather than trusting the init script's exit code) and
`verify_airflow_auth` (parses the actual JWT response body rather than just checking HTTP 200).
**Apply to:** `verify_data_write_access()` — write a real probe file, read it back, assert content,
then delete it; never a bits-only `os.access()`/`stat()` check.

### Assert-based failure convention + `OK:`/`FAILED:` print output
**Source:** `scripts/verify_environment.py` throughout (`assert not missing, f"..."` style; `main()`
prints `"OK: ..."` after each check; `__main__` catches `AssertionError` and prints `"FAILED: ..."`)
**Apply to:** Both new functions in `scripts/verify_environment.py` — no new exception types, no
new output convention; reuse exactly what's there.

### `verify-phaseN` Makefile target accretion
**Source:** `Makefile` lines 39-110 (`verify-phase2` through `verify-phase7`, each following the
prior phase's shape and adding exactly what's new)
**Apply to:** `verify-phase8` — thin wrapper, no new pytest suite (RESEARCH.md confirms this phase
introduces no new unit-testable logic), just the extended `scripts/verify_environment.py` run; add
to `.PHONY:` line 1.

### `${AIRFLOW_UID:-50000}` / `.env`-sourced value convention
**Source:** `docker-compose.yml` line 93 (`user: "${AIRFLOW_UID:-50000}:0"`) and lines 68-69
(`ORACLE_APP_USER: "${ORACLE_APP_USER:-admin}"`)
**Apply to:** The `airflow-init` `command:` block's `chown ${AIRFLOW_UID:-50000}:0 ...` — reuse the
exact same `${VAR:-default}` shell-substitution shape already used project-wide for this identical
UID value, don't hardcode `50000`.

## No Analog Found

None — every file this phase touches already exists in the codebase (RESEARCH.md confirms "no new
files/directories"), so each file's own pre-existing conventions serve as its primary analog. No
file requires falling back to a purely external/RESEARCH.md-only pattern.

## Metadata

**Analog search scope:** `docker-compose.yml`, `docker/airflow/Dockerfile`,
`scripts/verify_environment.py`, `Makefile`, `docs/environment.md`, `generator/generate_csv.py`,
`airflow/dags/_common/paths.py`, `.github/workflows/ci.yml` — all read directly in full or by
targeted grep+read (all are small, single-digit-KB files; no file exceeded 250 lines, so no
offset/limit chunking was needed).
**Files scanned:** 8
**Pattern extraction date:** 2026-09-01
