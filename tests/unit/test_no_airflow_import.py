"""ENGINE-09 enforcement: no file under ``csv_processor`` may import
anything from the ``airflow`` namespace, directly or transitively
(03-05-PLAN.md Task 3).

An AST-based scan, not a human grep -- ``_imports_airflow()`` is proven
against a synthetic ``import airflow`` file (never committed) before it is
trusted against the real package tree, so this test can't be vacuously
passing a scanner that always returns ``False``.
"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE_ROOT = Path("packages/csv-processor/src/csv_processor")


def _imports_airflow(py_file: Path) -> bool:
    """True if ``py_file`` contains an ``import`` or ``from ... import ...``
    naming the ``airflow`` module or any ``airflow.*`` submodule (covers
    both a bare ``import airflow`` and any ``apache-airflow-providers-*``
    package, which installs under the ``airflow.providers.*`` namespace).
    """
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "airflow" or alias.name.startswith("airflow."):
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module is not None and (
                node.module == "airflow" or node.module.startswith("airflow.")
            ):
                return True
    return False


def test_scanner_detects_a_synthetic_airflow_import(tmp_path: Path) -> None:
    """Self-test: proves the scanner has real detection power, not just a
    function that always returns False."""
    synthetic = tmp_path / "would_be_offender.py"
    synthetic.write_text("import airflow\n", encoding="utf-8")

    assert _imports_airflow(synthetic) is True


def test_scanner_detects_a_synthetic_from_airflow_import(tmp_path: Path) -> None:
    """Also covers the `from airflow.X import Y` / provider-package shape."""
    synthetic = tmp_path / "would_be_offender_from.py"
    synthetic.write_text(
        "from airflow.providers.oracle.hooks.oracle import OracleHook\n", encoding="utf-8"
    )

    assert _imports_airflow(synthetic) is True


def test_scanner_does_not_flag_an_ordinary_stdlib_import(tmp_path: Path) -> None:
    """Negative control: an unrelated import must never be flagged."""
    innocent = tmp_path / "innocent.py"
    innocent.write_text("import csv\nfrom pathlib import Path\n", encoding="utf-8")

    assert _imports_airflow(innocent) is False


def test_no_csv_processor_module_imports_airflow() -> None:
    """Every real file in the package -- one assertion per file, so a
    failure names the exact offending file."""
    py_files = sorted(_PACKAGE_ROOT.rglob("*.py"))
    assert py_files, f"expected to find .py files under {_PACKAGE_ROOT}, found none"

    for py_file in py_files:
        assert not _imports_airflow(py_file), py_file
