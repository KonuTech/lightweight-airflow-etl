# Phase 2: Config Contract & CSV Generator - Pattern Map

**Mapped:** 2026-08-28
**Files analyzed:** 16 new + 3 modified
**Analogs found:** 14 / 16 (from the reference repo; two greenfield with no analog: `configs/*.json`
data files themselves and the RLIMIT_AS memory test)

This project's own codebase has almost no source code yet (Phase 1 only produced infra: Makefile,
docker-compose, DDL, one stdlib-`unittest` test, `scripts/verify_environment.py`). Per
CLAUDE.md/CONTEXT.md's explicit two-tier reuse strategy, every analog below is the reference repo at
`/home/user/projects/airflow-platform` (Tier B: read algorithm, adapt scope — never imported).

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/csv-processor/src/csv_processor/config/models.py` | model | transform (validate dict→typed) | `packages/dataplat/src/dataplat/config/model.py` | exact |
| `packages/csv-processor/src/csv_processor/config/loader.py` | utility | transform (load+merge+validate) | `packages/dataplat/src/dataplat/config/loader.py` | exact |
| `packages/csv-processor/src/csv_processor/config/__init__.py` | config | n/a | `packages/dataplat/src/dataplat/config/__init__.py` | role-match |
| `configs/defaults.json` | config | n/a | `configs/defaults.yaml` (reference repo) | role-match (format differs: JSON vs YAML per D-04/D-05) |
| `configs/datasets/customers.json` | config | n/a | `configs/datasets/customers.yaml` (reference repo) | role-match (shape only — column set is this project's own, see DDL) |
| `configs/datasets/orders.json` | config | n/a | `configs/datasets/orders.yaml` (reference repo) | role-match |
| `generator/generate_csv.py` | utility | batch (file generation) | none in reference repo (business-row generator is new authorship per STACK.md) | no analog — pattern from RESEARCH.md's "Pattern 3" only |
| `tools/corpus/manifest.py` | model | transform (YAML→validated dataclasses) | `tools/corpus/manifest.py` (reference repo) | exact — Tier B port, scope-trimmed |
| `tools/corpus/generators.py` | utility | file-I/O (byte-exact generation) | `tools/corpus/generators.py` (reference repo) | exact — Tier B port, R1-R10 verbatim |
| `tools/corpus/digests.py` | utility | file-I/O (hashing) | `tools/corpus/digests.py` (reference repo) | exact — near-verbatim port candidate |
| `tools/corpus/__main__.py` | utility | request-response (CLI) | `tools/corpus/__main__.py` (reference repo) | exact — near-verbatim port candidate |
| `tools/corpus/__init__.py` | config | n/a | `tools/corpus/__init__.py` (reference repo) | exact |
| `tests/fixtures/corpus.yaml` | config | n/a | `tests/fixtures/corpus.yaml` (reference repo) | role-match (content scoped down to ~25-30 fixtures) |
| `tests/unit/test_config_models.py` | test | n/a | no direct file; reference repo's `packages/dataplat/tests/unit/test_config_model.py`-style tests (not read this session; inferred from D-17) | partial |
| `tests/unit/test_config_loader.py` | test | n/a | reference repo's config loader tests (same caveat) | partial |
| `tests/unit/test_generate_csv.py` | test | n/a | none — new authorship | no analog |
| `tests/unit/test_corpus_bounded_memory.py` | test | event-driven (subprocess) | none — ADR describes technique only, no implementation exists | no analog (see RESEARCH.md Assumption A4) |
| `Makefile` (modified) | config | n/a | this project's own `Makefile` (Phase 1) | exact — extend existing file, same convention |
| `docker-compose.yml` (modified) | config | n/a | this project's own `docker-compose.yml` (Phase 1) | exact — extend existing file, same convention |
| `pyproject.toml` / `packages/csv-processor/pyproject.toml` / `docker/airflow/Dockerfile` (modified) | config | n/a | this project's own three files (Phase 1) | exact |

## Pattern Assignments

### `packages/csv-processor/src/csv_processor/config/models.py` (model, transform)

**Analog:** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/config/model.py`
(785 lines — read `ColumnContract` lines 188-271 and `CsvParsingConfig` lines 362-412 this session;
every model class uses the identical `model_config` line, confirmed by grepping all 13 class
definitions in that file).

**Frozen, extra-forbid pattern** (verified across every one of the 13 model classes in the analog,
e.g. lines 260, 403, 686):
```python
model_config = ConfigDict(extra="forbid", frozen=True)
```
Apply this exact line, unmodified, to every model class this phase defines (`ColumnSpec`,
`CsvDialectConfig`, `OracleTargetSpec`, `ProcessingConfig`, `DatasetConfig`).

**Column-type Literal pattern** (analog lines 200-209, 260-271 — `ColumnContract`):
```python
name: str
type: _COLUMN_TYPES        # closed Literal, not a bare str — catches typos as validation errors
nullable: bool
required: bool              # kept distinct from `nullable`, never collapsed (mirrors D-09)
format: str | None = None   # strptime string, only meaningful for date/timestamp
```
This project's own `ColumnSpec` (RESEARCH.md's skeleton) additionally needs `precision`/`scale`
(D-10, not present in the analog since the reference repo's numeric handling lives elsewhere) —
add them as `int | None = None` following the same "None means not applicable to this type" idiom
already used for `format`.

**CSV dialect config pattern** (analog lines 403-412 — note the reference repo's version is
detect-or-override shaped, `str | None = None`; this project's D-01 instead wants every field to
always have a concrete value with sensible defaults, so adapt the *shape* — one flat frozen model,
one field per dialect concern — but change every default from `None` to a real value per D-01/D-02):
```python
model_config = ConfigDict(extra="forbid", frozen=True)
encoding: str | None = None
delimiter: str | None = None
quotechar: str = '"'
header_row: int | None = None
```
becomes (this project's own shape, D-02's additive fields):
```python
model_config = ConfigDict(extra="forbid", frozen=True)
delimiter: str = ","
encoding: str = "utf-8"
quotechar: str = '"'
header: bool = True
escapechar: str | None = None
doublequote: bool = True
lineterminator: str = "\n"
```

**Nested composition pattern** (analog's top-level `DatasetConfig`, lines 622-686 — composes every
sub-model as a typed field, never a raw dict):
```python
class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    dataset: str
    file_pattern: str
    csv: CsvDialectConfig = Field(default_factory=CsvDialectConfig)
    columns: list[ColumnSpec]
    oracle: OracleTargetSpec
    processing: ProcessingConfig
```

---

### `packages/csv-processor/src/csv_processor/config/loader.py` (utility, transform)

**Analog:** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/config/loader.py`
(71 lines, read in full this session).

**Imports pattern** (lines 13-31):
```python
from __future__ import annotations
from typing import TYPE_CHECKING, Any
import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError
from dataplat.config.model import DatasetConfig
from dataplat.errors import ConfigurationError
if TYPE_CHECKING:
    from pathlib import Path
```
Adapt: swap `yaml.safe_load` for stdlib `json.loads` (D-04/D-05 use `.json`, not `.yaml` — no new
dependency needed for this file), and import `DatasetConfig`/a local `ConfigurationError` from this
project's own `csv_processor.config.models`/`csv_processor.config.errors` instead of `dataplat.*`
(the one-line import swap Tier-A/Tier-B files consistently need per CLAUDE.md's reuse discipline).

**Core merge+validate+re-raise pattern** (lines 39-71, read in full — this is the load-bearing
excerpt for CONFIG-02):
```python
def load_config(path: Path, *, defaults_path: Path) -> DatasetConfig:
    defaults = _load_yaml_mapping(defaults_path)   # -> json.loads(...) in this project
    dataset = _load_yaml_mapping(path)
    merged = {**defaults, **dataset}   # shallow, top-level only -- see Pitfall 3
    try:
        return DatasetConfig.model_validate(merged)
    except ValidationError as exc:
        msg = f"invalid dataset config at {path}: {exc}"
        raise ConfigurationError(
            msg,
            context={"path": str(path), "errors": exc.errors()},
        ) from exc
```
This is copy-adapt-ready almost verbatim — only the file format (`json.loads` vs `yaml.safe_load`)
and the exception's origin module change. `exc.errors()` is what satisfies CONFIG-02's "complete
list of errors in one pass" with zero custom accumulation code.

**Error handling pattern:** `ConfigurationError` must be a plain `Exception` subclass carrying a
`context: dict` attribute (mirrors `dataplat.errors.ConfigurationError`, not read directly this
session but its call signature — `ConfigurationError(msg, context={...})` — is fully visible in the
loader excerpt above); define it locally in `csv_processor/config/errors.py` or inline in `loader.py`,
never imported from `dataplat`.

---

### `configs/defaults.json` / `configs/datasets/{customers,orders}.json` (config, n/a)

**Analog:** `/home/user/projects/airflow-platform/configs/datasets/customers.yaml`,
`orders.yaml`, `defaults.yaml` (shape reference only, per CONTEXT.md's explicit instruction not to
copy their `quality:`/`scd:`/`retention:`/`freshness:`/`reconciliation:` blocks — out of scope here).

**What to take:** the *idea* of `columns:` as a list of `{name, type, nullable, required, format}`
dicts, and `defaults.yaml`'s convention of holding only genuinely-shared top-level keys (not a
partially-overridden nested block — see Pitfall 3 in RESEARCH.md).

**What to build instead (this project's own, DDL-verified shape):** Use RESEARCH.md's "Derived
column contract" table verbatim — it is already cross-checked against
`docker/oracle/init/02_customers.sql:11-19` and `03_orders.sql:11-17`, which are this project's real
source of truth, not the reference repo's YAML. Do not copy the reference repo's actual column
names/types; only its structural idea of a schema block.

---

### `generator/generate_csv.py` (utility, batch)

**No analog exists** — STACK.md/RESEARCH.md's "Pattern 3" is the sole source. Zero coupling to
`csv_processor` per D-14's own explicit constraint (reads `csv_processor.config.loader.load_config`
only to learn column shape, never imports `detect`/`validate`/`normalize`). Use stdlib `csv.writer`
(RFC-4180-aware), `Faker.seed(seed)` for realistic fields, and a separate `random.Random(seed)`
instance for invalid-row category selection (D-15's four categories) and numeric/date ranges — see
RESEARCH.md's "Code Examples" and "Pattern 3" sections for the full spec; no reference-repo file
provides a directly portable generator.

---

### `tools/corpus/manifest.py`, `generators.py`, `digests.py`, `__main__.py` (D-16 corpus subsystem)

**Analogs:** the reference repo's own `tools/corpus/{manifest,generators,digests,__main__}.py`,
read directly this session (manifest.py lines 1-120; generators.py lines 1-150; digests.py in full;
__main__.py in full). D-16e locks the exact same path (`tools/corpus/`) specifically so these stay
line-for-line cross-referenceable.

**`digests.py` — near-verbatim port** (entire 141-line file read; every function
(`sha256_file`, `qualify`, `format_digests`, `parse_digests`, `read_digests`, `write_digests`) is
pure stdlib `hashlib`/`Mapping` manipulation with zero `dataplat` coupling — copy essentially
unchanged):
```python
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()

def format_digests(digests: Mapping[str, str]) -> str:
    return "".join(f"{digest}{_SEPARATOR}{name}\n" for name, digest in digests.items())
```
Note the `sha256sum`-format contract: two spaces between digest and name (`_SEPARATOR = "  "`), names
relative to the repo root (D-16e already locks this) — this is what lets
`sha256sum -c tests/fixtures/CORPUS.sha256` work as an independent check.

**`__main__.py` — near-verbatim port, argparse subparsers shape** (168 lines, read in full):
```python
subparsers = parser.add_subparsers(dest="command", required=True)
generate = subparsers.add_parser("generate", help="materialise the corpus")
generate.add_argument("--manifest", type=Path, default=_DEFAULT_MANIFEST)
generate.add_argument("--out", type=Path, default=_DEFAULT_OUT)
generate.add_argument("--write-digests", type=Path, default=None, ...)
```
`command_verify` regenerates into a `tempfile.TemporaryDirectory` and diffs against the committed
oracle **without ever reading the on-disk corpus** — this exact "generate to temp, compare, never
touch the real oracle on verify" structure is what `make fixtures-verify` (D-16f) must wrap. Port
`build_parser`/`command_generate`/`command_verify`/`main` essentially unchanged; only trim the
`--fast`/large-profile-skip flag if this project's scoped-down corpus has no need for it (optional,
Claude's discretion).

**`generators.py` — R1-R10 determinism rules, ported with content trimmed** (531 lines total; lines
1-150 read this session, covering the full module docstring's ten rules plus the `stream_for`
function):
```python
def stream_for(master_seed: str, name: str) -> random.Random:
    digest = hashlib.sha256(f"{master_seed}|{name}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))  # noqa: S311
```
This is R1's exact mechanism, load-bearing per CONTEXT.md's canonical_refs — reproduce verbatim.
Also carries forward: `_FLUSH_PARTS`-style batched writes for the large fixture (never materialize
whole in memory), `errors="strict"` on every encoder, gzip's `mtime=0`/`filename=""` and zip's
`date_time=(1980,1,1,0,0,0)` (R5, already quoted in full in RESEARCH.md's own Code Examples — no
need to re-read, just apply). **Trim:** drop any generator kind/fixture category tied to this
project's out-of-scope SCD/locale/quarantine/referential vocabulary (per D-16a's scoping instruction
and RESEARCH.md's "Recommended scoped fixture category list").

**`manifest.py` — model shape ported, `expect:` block kept permissive** (first 120 of 1216 lines
read this session, covering the module docstring's deserialization-safety rationale and the
`_FIXTURE_KEY_ORDER`/`_COLUMN_KEY_ORDER` declared-order constants):
```python
import yaml  # type: ignore[import-untyped]
...
GeneratorKind = Literal["tabular", "literal", "literal_unicode", "wrapper", "multipart"]
_FIXTURE_KEY_ORDER: Final[tuple[str, ...]] = ("name", "covers", "generator", ...)
```
Key points to reproduce: (1) `yaml.safe_load` only, never `yaml.load` (V5/T-01-13 threat, called out
in the analog's own module docstring); (2) every model frozen + `extra` forbidden at the outer
schema level; (3) the one deliberate relaxation — unknown keys *inside* `expect:` are accepted
(D-16d's permissive prose requirement, matches this analog's own precedent exactly, not something
this project invented). Full 1216-line file not read beyond line 120; the planner/implementer should
do a targeted second read (grep for `class Fixture`, `def load_manifest`) when authoring the
scoped-down model, since the file's constants confirm the four `row_spec` column kinds
(`zero_padded_int`, `pick`, `decimal`, `repeat`) match RESEARCH.md's own summary exactly.

---

### `tests/unit/test_config_models.py`, `test_config_loader.py` (test, n/a)

**No specific reference-repo test file was read this session** (out of budget — RESEARCH.md's
"Validation Architecture" section already fully specifies the required behavior per test file: valid
config round-trip, precision/scale rejection, delimiter/decimal-separator collision, nullable/
required combinations for `test_config_models.py`; success/failure/multi-error-aggregation paths for
`test_config_loader.py`). Structure both as stdlib `unittest.TestCase` classes matching this
project's own existing convention in `tests/test_verify_environment.py` (see Shared Patterns below)
unless Phase 2's own planning decides to adopt `pytest`-native functions instead (RESEARCH.md
recommends `pytest` as the new, not-yet-installed framework — Wave 0 gap).

---

## Shared Patterns

### Frozen, `extra="forbid"` Pydantic v2 models
**Source:** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/config/model.py`
(every one of its 13 classes)
**Apply to:** `ColumnSpec`, `CsvDialectConfig`, `OracleTargetSpec`, `ProcessingConfig`,
`DatasetConfig` in `packages/csv-processor/src/csv_processor/config/models.py`
```python
model_config = ConfigDict(extra="forbid", frozen=True)
```

### Config load → merge → validate → re-raise as one exception type
**Source:** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/config/loader.py:39-72`
**Apply to:** `csv_processor.config.loader.load_config()`
```python
merged = {**defaults, **dataset}
try:
    return DatasetConfig.model_validate(merged)
except ValidationError as exc:
    raise ConfigurationError(f"invalid dataset config at {path}: {exc}",
                              context={"path": str(path), "errors": exc.errors()}) from exc
```

### `sha256sum`-compatible digest oracle format
**Source:** `/home/user/projects/airflow-platform/tools/corpus/digests.py` (full file)
**Apply to:** `tools/corpus/digests.py`, `tests/fixtures/CORPUS.sha256`
```python
_SEPARATOR: Final = "  "   # two spaces — GNU coreutils text-mode form
f"{digest}{_SEPARATOR}{name}\n"
```

### Per-fixture RNG derivation (R1)
**Source:** `/home/user/projects/airflow-platform/tools/corpus/generators.py:127-139`
**Apply to:** every fixture generator function in `tools/corpus/generators.py`
```python
digest = hashlib.sha256(f"{master_seed}|{name}".encode()).digest()
random.Random(int.from_bytes(digest, "big"))
```

### Makefile as project-wide command entrypoint
**Source:** this project's own `/home/user/projects/lightweight-airflow-etl/Makefile` (Phase 1)
**Apply to:** new `fixtures`/`fixtures-verify`/`generate`/`verify-phase2` targets (D-16f/D-16g) —
extend the existing `.PHONY:` line and follow the existing `##`-commented target style, e.g.:
```makefile
fixtures-verify:   ## Regenerate corpus to a temp dir, diff SHA-256 against the committed oracle
	uv run python -m tools.corpus verify
```

### Three-place dependency declaration (project-specific pitfall, not a code pattern but load-bearing)
**Source:** RESEARCH.md Pitfall 1, verified against `packages/csv-processor/pyproject.toml:10`
(`dependencies = []`) and `docker/airflow/Dockerfile:3-8` (`pip install --no-deps
packages/csv-processor/`)
**Apply to:** any plan task that adds `pydantic`/`Faker`/`PyYAML` — must touch root `pyproject.toml`
and, only if the new import must run *inside the Airflow container*, also
`docker/airflow/Dockerfile`'s explicit `pip install` line (already has `pydantic==2.13.4`; Faker/
PyYAML are dev/test-only and do NOT need to go in the Dockerfile per RESEARCH.md's install-list
recommendation).

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `generator/generate_csv.py` | utility | batch | No business-row generator exists in the reference repo at all — RESEARCH.md's "Pattern 3" (Faker + `random.Random(seed)`) is the only guidance; new authorship |
| `tests/unit/test_corpus_bounded_memory.py` | test | event-driven (subprocess) | Reference repo's ADR-0005 *describes* the `RLIMIT_AS` technique but has **no committed implementation** (verified: only a manifest-level `approx_bytes > 2 * rlimit_as_bytes` static check exists in `tests/unit/test_corpus_manifest.py:146-173`, not a live memory test) — see RESEARCH.md's own code sketch under "RLIMIT_AS bounded-memory technique" for the only available reference, flagged there as new authorship (Assumption A4) |
| `configs/datasets/{customers,orders}.json` (the actual column *values*, not the model shape) | config | n/a | Content must be derived from this project's own Oracle DDL (`docker/oracle/init/02_customers.sql`, `03_orders.sql`), not copied from the reference repo's `.yaml` files, which encode an out-of-scope superset schema |
| `tests/unit/test_generate_csv.py` | test | n/a | Tests a generator with no existing analog; author fresh per RESEARCH.md's determinism/ratio requirements |

## Metadata

**Analog search scope:** `/home/user/projects/airflow-platform` (`packages/dataplat/src/dataplat/config/`,
`packages/csv-processor/src/csv_processor/detect/dialect.py`, `tools/corpus/`,
`configs/datasets/*.yaml`, `docs/adr/0005-*.md`); this project's own `Makefile`, `docker-compose.yml`,
`docker/airflow/Dockerfile`, `pyproject.toml`, `packages/csv-processor/pyproject.toml`,
`tests/test_verify_environment.py`, `docker/oracle/init/{02_customers,03_orders}.sql`
**Files scanned:** 12 reference-repo files read directly this session (full or partial); 8 files in
this project's own repo read for existing-convention analogs
**Pattern extraction date:** 2026-08-28
