"""``Status`` enum and ``ProcessingResult`` -- the public data contract that
crosses the DAG<->engine boundary (ENGINE-08).

``Status`` is a closed, 7-member enum copied verbatim from
``REQUIREMENTS.md`` -- adding an 8th member (e.g. an "already processed"
status for D-01's idempotency short-circuit) is explicitly out of scope for
this phase; a re-processed file returns the ORIGINAL recorded status instead.

``ProcessingResult`` follows ``csv_processor.config.models``'s frozen,
extra-forbid convention (every model in this codebase rejects unknown keys
and cannot be mutated after construction) so it round-trips safely through
Airflow's XCom as ``result.model_dump(mode="json")`` (ARCHITECTURE.md Pattern
3) without ever risking a silent typo'd field.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict


class Status(str, Enum):
    """The complete, closed set of ``ProcessingResult.status`` values
    (ENGINE-08). No 8th member -- see module docstring.
    """

    SUCCESS = "SUCCESS"
    SUCCESS_WITH_INVALID_ROWS = "SUCCESS_WITH_INVALID_ROWS"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    INVALID_FILE = "INVALID_FILE"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"


class ProcessingResult(BaseModel):
    """The one public entrypoint's (``csv_processor.engine.process()``,
    Plan 04-02) return value -- a summary only, never per-row detail
    (ARCHITECTURE.md Pattern 3).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Status
    dataset: str
    file_name: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    duration_seconds: float
    checksum: str | None = None  # None on FILE_NOT_FOUND/CONFIGURATION_ERROR (never computed)
