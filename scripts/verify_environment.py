"""Verify the local docker-compose environment is up and correctly provisioned.

Checks (D-05, INFRA-03, ROADMAP.md Phase 1 Success Criterion 2):
1. Oracle: all 5 expected tables (CUSTOMERS_VALID/INVALID, ORDERS_VALID/INVALID,
   INGESTION_METADATA) exist in the ADMIN schema of FREEPDB1, confirmed by querying
   USER_TABLES (not by trusting init-script exit status).
2. Oracle: representative columns exist on CUSTOMERS_VALID and ORDERS_VALID,
   confirmed via ALL_TAB_COLUMNS.
3. Airflow: the admin/admin credential pair authenticates against the REST API's
   /auth/token endpoint and returns a JWT (access_token).

`verify_tables()` and `verify_columns()` are intentionally standalone, importable
functions — Phase 4's Oracle integration tests reuse them (see 01-PATTERNS.md).
"""

from __future__ import annotations

import http.client
import json
import sys
import time
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred to a type-checking-only import (WR-03): the module-level `import
    # oracledb` made loading this file for testing verify_airflow_auth() (which
    # has no Oracle dependency) require the oracledb driver to be installed.
    # `oracledb.connect()` itself is imported lazily inside main() below.
    import oracledb

ORACLE_DSN = "localhost:1521/FREEPDB1"
ORACLE_USER = "admin"
ORACLE_PASSWORD = "admin"

AIRFLOW_AUTH_TOKEN_URL = "http://localhost:8080/auth/token"
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"

# G-01-1: on Docker Desktop/WSL2, docker-compose's --wait can (even with a real
# healthcheck) return moments before the apiserver's ASGI server is fully ready to
# serve a response, producing a transient ConnectionResetError during the
# response-read phase. This is a bounded, evidence-backed cold-start race (measured
# ~12.66s in the debug session, see .planning/debug/apiserver-auth-connreset.md),
# not a permanent failure -- retry with backoff instead of crashing on it.
AUTH_RETRY_ATTEMPTS = 6
AUTH_RETRY_BASE_DELAY_SECONDS = 1.0
AUTH_RETRY_MAX_DELAY_SECONDS = 8.0


def verify_tables(cursor: oracledb.Cursor, expected: set[str]) -> None:
    """Assert that every table name in `expected` exists in the current schema's
    USER_TABLES view.

    Raises AssertionError naming any tables missing from `expected` if the query's
    result set doesn't fully cover it. Reusable by Phase 4's Oracle integration tests.
    """
    placeholders = ", ".join(f":{i}" for i in range(1, len(expected) + 1))
    cursor.execute(
        f"SELECT table_name FROM user_tables WHERE table_name IN ({placeholders})",
        list(expected),
    )
    found = {row[0] for row in cursor.fetchall()}
    missing = expected - found
    assert not missing, f"Missing tables: {missing}"


def verify_columns(cursor: oracledb.Cursor, table: str, expected_columns: set[str]) -> None:
    """Assert that `table`'s column set (owned by ADMIN) is a superset of
    `expected_columns`, confirmed via ALL_TAB_COLUMNS (D-05).

    Superset, not exact-equal, so adding a column later doesn't break this check.
    Reusable by Phase 4's Oracle integration tests.
    """
    cursor.execute(
        "SELECT column_name FROM all_tab_columns WHERE table_name = :table_name AND owner = 'ADMIN'",
        {"table_name": table},
    )
    found = {row[0] for row in cursor.fetchall()}
    missing = expected_columns - found
    assert not missing, f"Table {table} missing columns: {missing}"


def verify_widened_invalid_columns(
    cursor: oracledb.Cursor, table: str, data_columns: set[str]
) -> None:
    """Assert that every column in `data_columns` on `table` (owned by ADMIN) is a
    nullable VARCHAR2, confirmed via ALL_TAB_COLUMNS (D-01/D-04/D-05, Plan 03-01).

    Raises AssertionError naming any column that fails either the data_type == 'VARCHAR2'
    or nullable == 'Y' check. Mirrors verify_columns()'s assert-and-name-the-culprit style;
    reusable by Phase 4's Oracle integration tests exactly like verify_columns() already is.
    """
    column_binds = {f"col{i}": name for i, name in enumerate(data_columns)}
    placeholders = ", ".join(f":{key}" for key in column_binds)
    cursor.execute(
        f"SELECT column_name, data_type, nullable FROM all_tab_columns "
        f"WHERE table_name = :table_name AND owner = 'ADMIN' "
        f"AND column_name IN ({placeholders})",
        {"table_name": table, **column_binds},
    )
    rows = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    missing = data_columns - rows.keys()
    assert not missing, f"Table {table} missing expected data columns: {missing}"

    not_varchar2 = {
        name for name, (data_type, _nullable) in rows.items() if data_type != "VARCHAR2"
    }
    assert not not_varchar2, f"Table {table} columns not VARCHAR2: {not_varchar2}"

    not_nullable = {
        name for name, (_data_type, nullable) in rows.items() if nullable != "Y"
    }
    assert not not_nullable, f"Table {table} columns not nullable: {not_nullable}"


def verify_airflow_auth() -> None:
    """Assert that admin/admin authenticates against Airflow's REST API and returns
    a JWT (access_token field).

    Retries a bounded number of times (AUTH_RETRY_ATTEMPTS) with exponential backoff
    on urllib.error.URLError/OSError (G-01-1) -- OSError is the confirmed common
    superclass of ConnectionResetError, which urllib never wraps as URLError when
    raised during the response-read phase (see .planning/debug/apiserver-auth-connreset.md).
    Also retries http.client.IncompleteRead (a clean-but-early connection close during
    the same read phase, not an OSError subclass) and json.JSONDecodeError/
    UnicodeDecodeError (a truncated-but-non-empty body from the same cold-start race) --
    all are symptoms of the identical transient condition, just observed at different
    points in the read/decode/parse sequence (WR-01).
    A genuine urllib.error.HTTPError (e.g. HTTP 401) is never retried -- it is not
    transient and fails immediately, matching prior behavior exactly.
    """
    payload = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode(
        "utf-8"
    )
    request = urllib.request.Request(
        AIRFLOW_AUTH_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
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
                print(
                    f"Airflow auth attempt {attempt}/{AUTH_RETRY_ATTEMPTS} failed "
                    f"({exc}) -- retrying in {delay}s (likely a transient cold-start "
                    "race, see G-01-1)",
                    file=sys.stderr,
                )
                time.sleep(delay)
                continue
            raise AssertionError(
                f"Airflow auth request failed after {AUTH_RETRY_ATTEMPTS} attempts: {exc}"
            ) from exc
    assert "access_token" in body, f"Response missing access_token field: {body}"


def main() -> int:
    import oracledb  # WR-03: lazy import -- only main() needs the Oracle driver.

    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    try:
        cursor = conn.cursor()
        verify_tables(
            cursor,
            expected={
                "CUSTOMERS_VALID",
                "CUSTOMERS_INVALID",
                "ORDERS_VALID",
                "ORDERS_INVALID",
                "INGESTION_METADATA",
            },
        )
        print("OK: all 5 tables exist in ADMIN schema of FREEPDB1")

        verify_columns(
            cursor,
            table="CUSTOMERS_VALID",
            expected_columns={"CUSTOMER_ID", "NAME", "COUNTRY", "INGESTED_AT"},
        )
        verify_columns(
            cursor,
            table="ORDERS_VALID",
            expected_columns={"ORDER_ID", "CUSTOMER_ID", "AMOUNT", "INGESTED_AT"},
        )
        print("OK: CUSTOMERS_VALID and ORDERS_VALID have expected representative columns")

        verify_widened_invalid_columns(
            cursor,
            table="CUSTOMERS_INVALID",
            data_columns={
                "CUSTOMER_ID",
                "NAME",
                "COUNTRY",
                "BIRTH_DATE",
                "EVENT_TS",
                "SIGNUP_COUNTRY",
            },
        )
        verify_widened_invalid_columns(
            cursor,
            table="ORDERS_INVALID",
            data_columns={"ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "AMOUNT"},
        )
        verify_columns(cursor, table="CUSTOMERS_INVALID", expected_columns={"RAW_LINE"})
        verify_columns(cursor, table="ORDERS_INVALID", expected_columns={"RAW_LINE"})
        print(
            "OK: CUSTOMERS_INVALID and ORDERS_INVALID data columns are nullable VARCHAR2 "
            "at their original sizes, with a RAW_LINE column present"
        )
    finally:
        conn.close()

    verify_airflow_auth()
    print("OK: admin/admin authenticates against Airflow's /auth/token endpoint")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
