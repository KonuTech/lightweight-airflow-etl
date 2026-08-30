---
phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
reviewed: 2026-08-29T18:02:10Z
depth: standard
files_reviewed: 13
files_reviewed_list:
  - Makefile
  - packages/csv-processor/pyproject.toml
  - packages/csv-processor/src/csv_processor/config/models.py
  - packages/csv-processor/src/csv_processor/engine.py
  - packages/csv-processor/src/csv_processor/load.py
  - packages/csv-processor/src/csv_processor/models.py
  - tests/integration/__init__.py
  - tests/integration/conftest.py
  - tests/integration/test_engine_process_oracle.py
  - tests/integration/test_load_oracle.py
  - tests/unit/test_config_models.py
  - tests/unit/test_engine_chunks.py
  - tests/unit/test_engine_process.py
findings:
  critical: 1
  warning: 5
  info: 1
  total: 7
status: issues_found
---

# Phase 4: Code Review Report

**Reviewed:** 2026-08-29T18:02:10Z
**Depth:** standard
**Files Reviewed:** 13
**Status:** issues_found

## Summary

Reviewed the Oracle bulk-load / idempotency / `process()` entrypoint slice against the four
focus areas called out in the review brief: SQL-injection safety, transaction/atomicity
correctness, idempotency correctness, and general quality.

The SQL-injection defenses (`is_safe_identifier()`, enforced both at Pydantic-validation time in
`config/models.py` and again defense-in-depth in `load.insert_rows()`) are correctly wired and
verified empirically to be complete for every dynamically-built INSERT statement in this codebase
— every identifier that reaches a SQL string is either a hardcoded literal or has passed the
allowlist regex at least once, and every value goes through bind parameters. No injection path
was found.

The transaction/atomicity design (`process()`: one connection, no intermediate commits across
chunk inserts + the `ingestion_metadata` insert, single `commit()` at the end, `rollback()` on the
various failure branches) is correct in the paths that are actually reachable by every automated
test in this phase, including the real-Oracle idempotency race-backstop path (`ORA-00001`).

However, tracing the `except oracledb.Error:` branch against `oracledb`'s real exception hierarchy
(verified empirically against the actually-installed `oracledb==4.0.2` package, not assumed)
surfaced a genuine crash bug: a failure inside `load.get_connection()` itself — the most ordinary
possible "Oracle is unreachable / credentials are wrong" failure — is not translated into
`Status.DATABASE_ERROR` as the docstring promises. It instead raises an unhandled `AttributeError`
out of `process()`, because that branch calls `connection.rollback()` unconditionally while
`connection` is still `None` at that point. No test in this phase's suite exercises this exact
scenario (the closest one, `test_unexpected_exception_returns_processing_error`, uses a
`RuntimeError` side effect, which does not enter this code path), so nothing caught it. This is a
BLOCKER.

Beyond that, five warnings that would degrade robustness, security posture, or config-contract
completeness are documented below, plus one info-level note.

## Critical Issues

### CR-01: `process()` crashes with `AttributeError` instead of returning `DATABASE_ERROR` when the Oracle connection itself cannot be opened

**File:** `packages/csv-processor/src/csv_processor/engine.py:209-308`

**Issue:**

`connection: oracledb.Connection | None = None` is declared before the `try` block, and
`connection = load.get_connection()` is the very first statement inside it. If `load.get_connection()`
raises (e.g. Oracle is down, the network is unreachable, or `ORACLE_APP_USER_PASSWORD` is wrong),
`connection` is never rebound and stays `None`.

Verified empirically against the real `oracledb==4.0.2` package: `oracledb.connect()` raises
`oracledb.OperationalError`, a subclass of `oracledb.DatabaseError`, a subclass of `oracledb.Error`:

```
$ python3 -c "
import oracledb
try:
    oracledb.connect(user='baduser', password='badpass', dsn='localhost:1521/doesnotexist', tcp_connect_timeout=2)
except Exception as e:
    print(type(e), type(e).__mro__)
"
<class 'oracledb.exceptions.OperationalError'> (OperationalError, DatabaseError, Error, Exception, BaseException, object)
```

So this failure is caught by `except oracledb.Error:` (engine.py:297-308), whose body is:

```python
except oracledb.Error:
    connection.rollback()  # type: ignore[union-attr]
    return _build_result(
        Status.DATABASE_ERROR,
        ...
    )
```

`connection` is `None` here, so `connection.rollback()` raises `AttributeError:
'NoneType' object has no attribute 'rollback'`. This `AttributeError` is not caught by any sibling
`except` clause (an exception raised *inside* an `except` block is not caught by that same `try`'s
other `except` clauses) — it propagates straight out of `process()`, through the `finally` (which
correctly no-ops on `connection is None`), and crashes the caller (the Airflow task, or any test
calling `process()` directly). This directly contradicts the function's own documented contract:
*"a `ProcessingResult` with exactly one of the 7 closed `Status` values -- never raises; every
exception this function's own sequence can produce is caught and translated into a status
instead."*

Contrast this with the generic `except Exception:` branch three lines below it (engine.py:309-314),
which correctly guards for exactly this case:

```python
except Exception:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:  # noqa: BLE001 -- best-effort rollback, never masks the real error
            pass
```

The `# type: ignore[union-attr]` comment on the buggy line is the type checker's own signal that
`connection` may be `None` there — it was suppressed rather than fixed.

No test in this phase's suite exercises this: `test_unexpected_exception_returns_processing_error`
(`tests/unit/test_engine_process.py:116-133`) patches `csv_processor.engine.load.get_connection`
with `side_effect=RuntimeError("boom")`, and `RuntimeError` is not an `oracledb.Error`, so it lands
in the generic (guarded) `except Exception:` branch instead, never touching the buggy branch.

**Fix:**

```python
except oracledb.Error:
    if connection is not None:
        connection.rollback()
    return _build_result(
        Status.DATABASE_ERROR,
        config,
        file_path,
        start,
        checksum=checksum,
        total_rows=total_rows,
        valid_rows=valid_count,
        invalid_rows=invalid_count,
    )
```

Add a regression test alongside `test_unexpected_exception_returns_processing_error` that patches
`csv_processor.engine.load.get_connection` with `side_effect=oracledb.OperationalError("ORA-12541: TNS:no listener")`
(or any real `oracledb.Error` subclass) and asserts `result.status == Status.DATABASE_ERROR` rather
than letting the `AttributeError` escape the test.

## Warnings

### WR-01: `except StructuralValidationError:` also performs an unguarded `connection.rollback()`

**File:** `packages/csv-processor/src/csv_processor/engine.py:294-296`

**Issue:** This branch is not exploitable today — `StructuralValidationError` can only be raised
from inside `process_chunks()`'s `source.prepare_source()` call, which only ever runs after
`connection = load.get_connection()` has already succeeded — so `connection` is guaranteed non-None
at this specific call site *by construction of the surrounding code's control flow*, not by any
guard local to this line. That's a fragile invariant: any future refactor that reorders
`process()`'s steps (e.g. computing the checksum or checking structural validity before opening the
connection) would silently reintroduce CR-01's exact crash here too, and nothing about this line
would signal the danger.

**Fix:** Apply the same `if connection is not None:` guard here for consistency and defense in
depth, even though it is currently unreachable with `connection is None`:

```python
except StructuralValidationError:
    if connection is not None:
        connection.rollback()
    return _build_result(Status.INVALID_FILE, config, file_path, start, checksum=checksum)
```

### WR-02: `ColumnSpec` validates `format` presence for date/timestamp but never forbids it elsewhere

**File:** `packages/csv-processor/src/csv_processor/config/models.py:67-85`

**Issue:** `_check_type_specific_fields` requires `format` to be set when `type` is `"date"` or
`"timestamp"`, and explicitly *forbids* `precision`/`scale` on any type other than `"decimal"` (the
`elif self.precision is not None or self.scale is not None: raise ValueError(...)` branch). There is
no equivalent `elif` forbidding `format` on `"string"`, `"integer"`, or `"boolean"` columns. A config
author who typos a column's `type` (e.g. means `"date"` but leaves `type: "string"` while still
setting `format: "%Y-%m-%d"` from a copy-paste) gets no validation error at all — the stray `format`
is silently accepted and ignored, exactly the class of typo'd-config bug this model tree is
otherwise built to catch at validation time (per the module docstring's stated goal: *"an
unrecognized/typo'd config key is a validation-time error rather than a silently-ignored one"*).
`tests/unit/test_config_models.py` has no test covering this asymmetry (it tests the decimal
forbid-branch via `TestExtraForbidEnforcement`-adjacent cases but nothing for stray `format`).

**Fix:**

```python
@model_validator(mode="after")
def _check_type_specific_fields(self) -> ColumnSpec:
    if self.type in ("date", "timestamp"):
        if not self.format:
            msg = f"column {self.name!r}: type {self.type!r} requires a non-empty 'format'"
            raise ValueError(msg)
    elif self.format is not None:
        msg = f"column {self.name!r}: 'format' is only valid for type 'date'/'timestamp'"
        raise ValueError(msg)
    if self.type == "decimal":
        ...
```

### WR-03: Oracle credentials silently fall back to a fixed default when env vars are unset

**File:** `packages/csv-processor/src/csv_processor/load.py:64-76`

**Issue:** `oracle_user()`/`oracle_password()` fall back to the literal `"admin"`/`"admin"` if
`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD` are not set in the environment. This is a documented,
deliberate dev-environment convenience (per this repo's own conventions and Phase 1 setup), so it is
not flagged as a hardcoded-secret leak. It is flagged because the fallback is silent: if these env
vars are simply missing or misspelled in a *non-dev* environment (a bad Airflow connection/variable
config, a broken `docker-compose.yml` override, etc.), `get_connection()` will happily attempt to
connect with weak, guessable credentials instead of failing fast with a clear
"credentials not configured" error — turning a config mistake into a silent security downgrade
rather than a loud failure.

**Fix:** At minimum, log a warning when either env var is absent and the default is used, so a
misconfigured non-dev deployment is observable rather than silent:

```python
def oracle_user() -> str:
    value = os.environ.get("ORACLE_APP_USER")
    if value is None:
        logger.warning("ORACLE_APP_USER not set; falling back to dev default 'admin'")
        return "admin"
    return value
```

### WR-04: `cursor` opened in `process()` is never explicitly closed

**File:** `packages/csv-processor/src/csv_processor/engine.py:213`

**Issue:** `cursor = connection.cursor()` is created once per `process()` call but is never
`.close()`d on any exit path (success, early-return-on-idempotency-hit, or any exception branch).
`connection.close()` in the `finally` block will implicitly invalidate the cursor, so this is not a
leak across the life of the connection, but it is inconsistent with the rest of the module's
resource-cleanup discipline (e.g. `tests/integration/conftest.py`'s `oracle_cursor` fixture
explicitly closes its cursor before closing its connection) and makes the exit paths marginally
harder to reason about.

**Fix:** Wrap the cursor lifetime in its own `try/finally` (or `with connection.cursor() as cursor:`,
which `oracledb.Cursor` supports as a context manager) nested inside the connection's `try/finally`.

### WR-05: No test exercises `process()`'s behavior when `load.get_connection()` raises a real `oracledb.Error`

**File:** `tests/unit/test_engine_process.py`

**Issue:** This is the coverage gap that let CR-01 ship. Every non-DB status path in this file is
otherwise well covered (`FILE_NOT_FOUND`, `CONFIGURATION_ERROR`, `INVALID_FILE`,
`PROCESSING_ERROR`), but the one test that patches `get_connection` to fail
(`test_unexpected_exception_returns_processing_error`) uses `RuntimeError`, which never reaches the
`except oracledb.Error:` branch. There is no test asserting `process()` returns `DATABASE_ERROR`
(not a crash) when connecting to Oracle itself fails.

**Fix:** Add a test mirroring `test_unexpected_exception_returns_processing_error` but with
`side_effect=oracledb.OperationalError(...)`, asserting `result.status == Status.DATABASE_ERROR` —
this test will fail against the current code (proving CR-01) and pass once CR-01's fix lands.

## Info

### IN-01: `# type: ignore[union-attr]` suppressions mask a real, later-realized bug

**File:** `packages/csv-processor/src/csv_processor/engine.py:295, 298`

**Issue:** Both `connection.rollback()  # type: ignore[union-attr]` lines suppress the type
checker's (correct) observation that `connection: oracledb.Connection | None` may be `None` at that
point, rather than resolving it with a guard. In the `except oracledb.Error:` case (CR-01) the
type checker was right and the suppression hid a real bug reachable in production.

**Fix:** Once CR-01/WR-01 are fixed with explicit `if connection is not None:` guards, both
`# type: ignore[union-attr]` comments become unnecessary and should be removed — their continued
presence elsewhere in this codebase would otherwise normalize suppressing this exact class of
signal.

---

_Reviewed: 2026-08-29T18:02:10Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
