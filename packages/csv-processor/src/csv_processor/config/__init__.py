"""Config-contract package (CONFIG-01/CONFIG-02): Pydantic v2 model tree +
``load_config()`` merge/validate loader for a dataset's ``config.json``.
"""

from __future__ import annotations

from csv_processor.config.errors import ConfigurationError
from csv_processor.config.loader import load_config
from csv_processor.config.models import (
    ColumnSpec,
    CsvDialectConfig,
    DatasetConfig,
    OracleTargetSpec,
    ProcessingConfig,
)

__all__ = [
    "ColumnSpec",
    "ConfigurationError",
    "CsvDialectConfig",
    "DatasetConfig",
    "OracleTargetSpec",
    "ProcessingConfig",
    "load_config",
]
