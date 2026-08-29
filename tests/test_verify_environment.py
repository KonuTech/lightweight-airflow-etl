"""Regression tests for scripts/verify_environment.py::verify_airflow_auth() (G-01-1).

Proves, deterministically via a mocked urllib.request.urlopen (no real network/timing
dependency), that:
  1. A transient cold-start ConnectionResetError/OSError is retried with backoff and,
     if a later attempt succeeds, verify_airflow_auth() returns normally.
  2. If every attempt in the retry budget fails, verify_airflow_auth() raises a clean
     AssertionError (never a raw ConnectionResetError/OSError) naming the underlying
     error, after exactly AUTH_RETRY_ATTEMPTS attempts.
  3. A genuine urllib.error.HTTPError (e.g. HTTP 401) is NOT retried -- it raises
     AssertionError immediately, after exactly one urlopen call.

This is the project's first test file. It intentionally uses only Python's stdlib
`unittest` + `unittest.mock` (no new dependency, no test framework choice preempted --
Phase 3's TEST-01 introduces the project's formal test suite).

`scripts/` has no `__init__.py` and is not an installed package, so the module under
test is loaded via importlib.util.spec_from_file_location rather than a normal import.
"""

from __future__ import annotations

import importlib.util
import io
import json
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch

_MODULE_PATH = Path(__file__).resolve().parent.parent / "scripts" / "verify_environment.py"
_SPEC = importlib.util.spec_from_file_location("verify_environment", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
verify_environment = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(verify_environment)


class _FakeResponse:
    """Minimal context-manager stand-in for urllib.request.urlopen()'s return value."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


class VerifyAirflowAuthTests(unittest.TestCase):
    def test_retries_transient_connection_reset_then_succeeds(self) -> None:
        """Two ConnectionResetErrors followed by a successful response: no exception
        raised, urlopen called exactly 3 times."""
        success_body = json.dumps({"access_token": "fake-jwt"}).encode("utf-8")
        mock_urlopen = Mock(
            side_effect=[
                ConnectionResetError(104, "Connection reset by peer"),
                ConnectionResetError(104, "Connection reset by peer"),
                _FakeResponse(success_body),
            ]
        )
        with (
            patch.object(verify_environment.urllib.request, "urlopen", mock_urlopen),
            patch.object(verify_environment.time, "sleep", Mock()),
        ):
            verify_environment.verify_airflow_auth()  # must not raise

        self.assertEqual(mock_urlopen.call_count, 3)

    def test_retry_budget_exhausted_raises_clean_assertion_error(self) -> None:
        """ConnectionResetError on every attempt exhausts the retry budget: raises
        AssertionError (not ConnectionResetError), urlopen called exactly
        AUTH_RETRY_ATTEMPTS times."""
        mock_urlopen = Mock(side_effect=ConnectionResetError(104, "Connection reset by peer"))
        with (
            patch.object(verify_environment.urllib.request, "urlopen", mock_urlopen),
            patch.object(verify_environment.time, "sleep", Mock()),
        ):
            with self.assertRaises(AssertionError) as ctx:
                verify_environment.verify_airflow_auth()

        self.assertIn("Connection reset by peer", str(ctx.exception))
        self.assertEqual(mock_urlopen.call_count, verify_environment.AUTH_RETRY_ATTEMPTS)

    def test_http_error_is_not_retried(self) -> None:
        """A genuine HTTPError (e.g. HTTP 401) fails immediately with no retry:
        AssertionError mentioning the status code, urlopen called exactly once."""
        http_error = urllib.error.HTTPError(
            url=verify_environment.AIRFLOW_AUTH_TOKEN_URL,
            code=401,
            msg="Unauthorized",
            hdrs=None,  # type: ignore[arg-type]  # HTTPError accepts None at runtime; typeshed's stub disagrees
            fp=io.BytesIO(b"Unauthorized"),
        )
        mock_urlopen = Mock(side_effect=http_error)
        with (
            patch.object(verify_environment.urllib.request, "urlopen", mock_urlopen),
            patch.object(verify_environment.time, "sleep", Mock()),
        ):
            with self.assertRaises(AssertionError) as ctx:
                verify_environment.verify_airflow_auth()

        self.assertIn("401", str(ctx.exception))
        self.assertEqual(mock_urlopen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
