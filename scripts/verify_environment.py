"""Verify the local docker-compose environment is up and correctly provisioned.

Checks (D-05, INFRA-03):
1. Oracle: the expected tables exist in the ADMIN schema of FREEPDB1, confirmed by
   querying USER_TABLES (not by trusting init-script exit status).
2. Airflow: the admin/admin credential pair authenticates against the REST API's
   /auth/token endpoint and returns a JWT (access_token).

`verify_tables()` is intentionally a standalone, importable function — Phase 4's
Oracle integration tests reuse it (see 01-PATTERNS.md).
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

import oracledb

ORACLE_DSN = "localhost:1521/FREEPDB1"
ORACLE_USER = "admin"
ORACLE_PASSWORD = "admin"

AIRFLOW_AUTH_TOKEN_URL = "http://localhost:8080/auth/token"
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"


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


def verify_airflow_auth() -> None:
    """Assert that admin/admin authenticates against Airflow's REST API and returns
    a JWT (access_token field)."""
    payload = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode(
        "utf-8"
    )
    request = urllib.request.Request(
        AIRFLOW_AUTH_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise AssertionError(
            f"Airflow auth request failed with HTTP {exc.code}: {exc.read().decode('utf-8')}"
        ) from exc
    assert "access_token" in body, f"Response missing access_token field: {body}"


def main() -> int:
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    try:
        cursor = conn.cursor()
        verify_tables(cursor, expected={"INGESTION_METADATA"})
        print("OK: INGESTION_METADATA table exists in ADMIN schema of FREEPDB1")
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
