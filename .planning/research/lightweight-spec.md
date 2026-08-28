I have a question. I need to create a new project of lightweight version for airflow platform. Below is a list of expectations. However I would like to use knowledge of current project to be always available for the new project of lightweight airflow platform. How to create a new project with access to current repo of quasi production airflow platform. Below is a PROMTP with expectations for lightweight version of airflow platform.

PROMPT:
Yes. Since the **95-point production-like project is already completed**, I would deliberately make this a **different project with a much smaller scope**. The goal should be to preserve the important Airflow/ETL concepts while removing Kubernetes, MinIO, Vault, CDC, SCD, complex orchestration, etc.

One correction first: **Oracle Database itself is not open source**. However, Oracle provides **Oracle AI Database Free**, including container images suitable for local development. Oracle documents running the Free image locally with Docker, and the official `oracle/docker-images` repository contains the container build/configuration material. ([Oracle][1])

I would define the new project around the following **60 lightweight expectations**.

---

# Lightweight Airflow CSV ETL Platform — Requirements

## 1. Project Goal

Build a **lightweight local Airflow ETL environment** focused on one concrete problem:

> Efficiently detect, parse, validate and load generated CSV files into Oracle Database using Airflow.

This project is intentionally simpler than the previous quasi-production platform.

The main goal is to develop a **reusable Python CSV processing engine** and demonstrate how Airflow should orchestrate it.

The architecture should remain small enough to understand and develop quickly.

---

# 2. Target Architecture

```text
                    HTTP
                     │
                     ▼
              Airflow DAG Trigger
                     │
                     ▼
                config.json
                     │
                     ▼
              File Availability
                     │
                     ▼
              CSV Processing Engine
                     │
          ┌──────────┴──────────┐
          │                     │
          ▼                     ▼
    Valid Records         Invalid Records
          │                     │
          ▼                     ▼
   Oracle VALID table    Oracle INVALID table
```

Infrastructure:

```text
Windows
   │
Docker Desktop
   │
   └── WSL2
        │
        ├── Airflow
        ├── Airflow metadata DB
        ├── Oracle Database Free
        └── CSV processing environment
```

The environment should be operated **from WSL**, not from the Windows filesystem.

---

# 3. Scope Reduction

Unlike the previous project, this environment does **not** need to implement:

* Kubernetes
* kind
* MinIO
* Vault
* CDC
* SCD
* complex data lake architecture
* distributed processing
* complex lineage
* production-grade observability stack
* complex schema registry
* multi-database warehouse architecture

The focus is:

```text
Airflow
+
Python
+
CSV
+
Validation
+
Oracle
+
Docker/WSL
```

---

# 4. Airflow

## 4.1 Airflow is the orchestrator

Airflow should orchestrate the process but should **not contain the CSV processing implementation**.

The DAG should be thin.

Preferred architecture:

```text
DAG
 │
 ├── configuration
 ├── file detection
 ├── processing task
 └── result handling
```

The actual processing should live in a reusable Python package.

---

# 5. HTTP DAG Trigger

The DAG must be triggerable through HTTP.

Example concept:

```text
HTTP request
     │
     ▼
Airflow API
     │
     ▼
DAG run
```

The HTTP request should be capable of passing runtime configuration where appropriate.

For example:

```json
{
  "dataset": "customers",
  "config": "configs/customers.json"
}
```

Do not hard-code the dataset directly into the DAG.

---

# 6. TaskFlow API

Use the **Airflow TaskFlow API** for the DAG implementation.

The DAG should consist of logically separated tasks such as:

```text
load_config()
      ↓
wait_for_file()
      ↓
process_csv()
      ↓
load_results()
      ↓
report_result()
```

The exact number of tasks should remain small.

Avoid artificially splitting every individual operation into an Airflow task.

---

# 7. Configuration-Driven Processing

The CSV processing behavior should be controlled by a JSON configuration file.

Example:

```json
{
  "dataset": "customers",
  "input_directory": "/data/input",
  "file_pattern": "customers_*.csv",

  "csv": {
    "delimiter": ";",
    "encoding": "utf-8",
    "header": true
  },

  "schema": {
    "customer_id": {
      "type": "integer",
      "nullable": false
    },
    "name": {
      "type": "string",
      "nullable": false
    },
    "birth_date": {
      "type": "date",
      "format": "YYYY-MM-DD"
    },
    "balance": {
      "type": "decimal"
    }
  }
}
```

The configuration should define the **contract** between the generated CSV and the processing engine.

---

# 8. Configuration Responsibilities

`config.json` should be capable of describing at least:

* dataset name
* input directory
* filename mask/pattern
* delimiter
* encoding
* header presence
* column order
* column names
* data types
* nullable/non-nullable fields
* date format
* numeric format
* Oracle target table
* invalid-record table
* processing options

Do not put processing logic into JSON.

---

# 9. File Pattern Matching

The engine should locate files according to configuration.

Examples:

```text
customers_*.csv
customers_20260828.csv
customers_*.CSV
```

Support at least:

* glob patterns
* optionally regular expressions

The pattern should be configurable rather than hard-coded.

---

# 10. File Availability

Before processing, Airflow must determine whether the expected CSV exists.

Example:

```text
DAG starts
   │
   ▼
Does CSV exist?
   │
   ├── NO → wait
   │
   └── YES
        │
        ▼
      process
```

---

# 11. Deferrable Operator / Trigger

Evaluate the use of a **Deferrable Operator or custom Trigger** for waiting for the file.

This is especially appropriate because waiting should not unnecessarily occupy an Airflow worker slot.

Airflow's current documentation explains that a deferred task releases the worker while the Triggerer handles the asynchronous waiting. ([Apache Airflow][2])

For this project:

```text
Airflow task
     │
     ▼
File not available
     │
     ▼
DEFER
     │
     ▼
Triggerer
     │
     │ async polling
     ▼
File appears
     │
     ▼
Resume task
```

The trigger should be asynchronous and must not perform blocking operations.

---

# 12. Do Not Overengineer File Waiting

For the lightweight version, the file-waiting mechanism should only answer:

> Is a file matching the configured pattern available?

It does not need to implement a complete enterprise eventing system.

---

# 13. Generated CSV Files

Create a **CSV generator** for development/testing.

The generator should produce deterministic test data according to the JSON schema.

Example:

```text
config.json
     │
     ▼
CSV Generator
     │
     ▼
customers_20260828.csv
```

---

# 14. Generated Data Types

Generated CSV files should intentionally contain:

* strings
* integers
* decimals
* dates
* timestamps
* nullable values where configured

Dates should be generated in the format expected by Oracle / the configured target schema.

For example:

```text
2026-08-28
```

or the exact Oracle-compatible representation selected for the project.

---

# 15. Valid and Invalid Test Data

The generator should be capable of producing both:

### Valid records

```text
123;John;2026-01-10;1234.50
```

### Invalid records

```text
ABC;John;2026-99-99;NOT_NUMBER
```

This allows the validator to be tested without manually creating CSV files.

---

# 16. CSV Processing Engine

The main deliverable should be a reusable Python library:

```text
csv_processor
```

The engine should expose a simple interface such as:

```python
processor.process(
    file_path=file,
    config=config
)
```

The Airflow DAG should call this engine rather than implement CSV parsing itself.

---

# 17. Processing Pipeline

The engine should implement approximately:

```text
File
 │
 ▼
Read
 │
 ▼
Parse
 │
 ▼
Validate structure
 │
 ▼
Validate types
 │
 ▼
Normalize
 │
 ▼
Split
 ├──────────────┐
 ▼              ▼
VALID          INVALID
```

---

# 18. Bulk Processing

Processing must be designed for **bulk operation**, not one-record-at-a-time processing.

Avoid:

```python
for row in rows:
    database.insert(row)
```

when this means one database round-trip per record.

Prefer:

```text
CSV
 ↓
chunks
 ↓
bulk validation
 ↓
bulk database insertion
```

---

# 19. Streaming / Generators

Consider Python generators for CSV reading.

For example:

```python
def read_csv(...) -> Iterator[Record]:
    ...
```

The objective is to avoid unnecessarily loading the complete file into memory.

However, do not introduce generators merely for stylistic reasons.

The implementation should balance:

* memory
* CPU
* throughput
* simplicity

---

# 20. Chunked Processing

For larger CSV files, process records in chunks.

Example:

```text
CSV
 │
 ├── chunk 1 — 10,000 rows
 ├── chunk 2 — 10,000 rows
 ├── chunk 3 — 10,000 rows
 └── ...
```

The chunk size should be configurable.

---

# 21. Oracle Bulk Loading

Use Oracle bulk insertion mechanisms rather than executing one SQL statement per record.

Evaluate appropriate Python Oracle tooling, particularly the modern Oracle Python driver.

The objective is:

```text
Python
   │
   ▼
batch of rows
   │
   ▼
Oracle
```

rather than:

```text
Python
 │
 ├── INSERT
 ├── INSERT
 ├── INSERT
 ├── INSERT
 └── ...
```

---

# 22. Oracle Database

Use **Oracle Database Free** for the lightweight local environment.

It is not open-source software, but Oracle provides a free edition suitable for local development/testing. Oracle currently provides Free container images, including lightweight variants. ([Oracle][1])

Use the official Oracle container ecosystem where practical. The referenced `oracle/docker-images` repository is the official source repository for Oracle container configurations and examples. ([GitHub][3])

---

# 23. Oracle Docker Deployment

Oracle should run as a Docker container under Docker Desktop.

Example architecture:

```text
Docker Desktop
      │
      ├── Airflow
      │
      ├── Airflow DB
      │
      └── Oracle Database Free
```

Persist Oracle data using a Docker volume.

---

# 24. Oracle Target Tables

Create two primary target tables:

```text
CSV_DATA_VALID
CSV_DATA_INVALID
```

or dataset-specific equivalents:

```text
CUSTOMERS_VALID
CUSTOMERS_INVALID
```

---

# 25. Valid Data

Rows that pass all required validation rules should be loaded into the valid table.

Example:

```text
CUSTOMERS_VALID
----------------
CUSTOMER_ID
NAME
BIRTH_DATE
BALANCE
...
```

---

# 26. Invalid Data

Rows failing validation should be stored separately.

The invalid table should contain both the original data and useful error metadata.

For example:

```text
CUSTOMERS_INVALID
-----------------
CUSTOMER_ID
NAME
BIRTH_DATE
BALANCE
ERROR_CODE
ERROR_MESSAGE
SOURCE_FILE
ROW_NUMBER
```

---

# 27. Preserve Invalid Records

Do not simply discard invalid records.

The system should allow someone to answer:

> Why was this particular row rejected?

---

# 28. Validation Categories

The lightweight validator should initially support:

### Structural

* incorrect number of columns
* missing columns
* unexpected columns

### Type

* invalid integer
* invalid decimal
* invalid date

### Nullability

* required field is empty

### Basic business rules

Where configured.

Do not implement the entire validation framework from the previous 95-point project.

---

# 29. Oracle-Compatible Dates

The test dataset should deliberately exercise Oracle date handling.

The configuration should specify the expected format.

Example:

```json
{
  "birth_date": {
    "type": "date",
    "format": "YYYY-MM-DD"
  }
}
```

Python should validate/convert the value appropriately before inserting it into Oracle.

---

# 30. Type Conversion

The processing engine should explicitly convert:

```text
CSV string
    ↓
Python type
    ↓
Oracle type
```

For example:

```text
"123"
 ↓
int
 ↓
NUMBER
```

```text
"2026-08-28"
 ↓
date
 ↓
DATE
```

Avoid relying on accidental implicit Oracle conversions.

---

# 31. Transaction Handling

Bulk loading should use sensible transaction boundaries.

For example:

```text
process chunk
     ↓
validate
     ↓
insert valid
     ↓
insert invalid
     ↓
commit
```

The exact transaction strategy should be chosen based on correctness and performance.

---

# 32. Invalid Row Isolation

An invalid row should not necessarily cause an entire CSV to fail.

Example:

```text
100,000 rows
       │
       ├── 99,850 valid
       └──    150 invalid
```

Expected result:

```text
VALID   → 99,850
INVALID → 150
```

The behavior should be configurable.

---

# 33. Processing Result

The CSV engine should return a structured processing result.

Example:

```python
ProcessingResult(
    total_rows=100_000,
    valid_rows=99_850,
    invalid_rows=150,
    processing_time=12.4,
)
```

This result can be consumed by Airflow.

---

# 34. Airflow Result Handling

The DAG should expose a concise processing summary.

Example:

```text
Dataset: customers
File: customers_20260828.csv

Rows:       100,000
Valid:       99,850
Invalid:        150
Duration:     12.4s
Status:       SUCCESS_WITH_ERRORS
```

---

# 35. Error Semantics

Distinguish:

```text
SUCCESS
SUCCESS_WITH_INVALID_ROWS
FILE_NOT_FOUND
INVALID_FILE
CONFIGURATION_ERROR
DATABASE_ERROR
PROCESSING_ERROR
```

Airflow task state should reflect the appropriate severity.

For example, a file containing 150 rejected rows might be a successful ingestion with data-quality errors rather than a technical task failure.

---

# 36. Idempotency — Lightweight Version

Even though this is a lightweight project, ingestion must not accidentally duplicate data when an Airflow task is retried.

At minimum identify the processed file using:

* filename
* file checksum
* dataset

A retry of the same file should be safe.

---

# 37. Processing Metadata

Maintain minimal ingestion metadata.

For example:

```text
file_name
file_checksum
dataset
processing_timestamp
total_rows
valid_rows
invalid_rows
status
```

This can live in Oracle.

Do not build a large metadata platform.

---

# 38. Duplicate File Detection

If the same file is encountered twice, the system should detect it.

Example:

```text
customers_20260828.csv
checksum = ABC123
```

appears again.

The system should not blindly load another copy.

---

# 39. Configuration Validation

Before processing a CSV, validate `config.json` itself.

Detect:

* missing fields
* invalid data types
* invalid table names
* invalid column definitions
* malformed JSON
* unsupported configuration options

A bad configuration should fail before CSV processing begins.

---

# 40. Python Environment

Create a dedicated Python environment/package for the CSV engine.

The environment should be reproducible.

Use a dependency file such as:

```text
pyproject.toml
```

Avoid installing arbitrary packages manually into the environment.

---

# 41. Python Type Hints

Use type hints throughout the CSV engine.

Example:

```python
def validate_row(
    row: CsvRow,
    schema: Schema
) -> ValidationResult:
    ...
```

---

# 42. Python Documentation

Document public:

* classes
* functions
* methods
* configuration models

Documentation should explain:

* purpose
* parameters
* return values
* exceptions

Keep it concise.

---

# 43. Python Logging

Use proper Python logging.

Do not use `print()` for application diagnostics.

Useful information:

```text
dataset
file
row/chunk
rows processed
valid rows
invalid rows
processing duration
Oracle operation
```

Do not log entire records unnecessarily.

---

# 44. Error Handling

Use explicit exceptions.

Examples:

```text
ConfigurationError
CsvParseError
ValidationError
OracleLoadError
FileProcessingError
```

Do not hide exceptions.

Airflow should receive meaningful failures.

---

# 45. Async — Evaluate, Don't Force

Consider asynchronous programming where it actually improves the architecture.

The most obvious candidate is **file availability polling / triggers**, because Airflow Triggers are explicitly asynchronous. ([Apache Airflow][2])

Do **not** automatically make CSV parsing or Oracle insertion asynchronous.

For CPU-bound CSV parsing and bulk database operations, synchronous/chunked processing may be simpler and faster.

---

# 46. Async File Trigger

If implementing a custom Airflow Trigger:

```python
async def run(self):
    while not file_exists():
        await asyncio.sleep(...)
```

The trigger must not perform blocking filesystem operations.

Airflow specifically requires trigger `run()` methods to be asynchronous and warns about blocking filesystem/network operations. ([Apache Airflow][2])

---

# 47. Resource Configuration

Docker Desktop resource allocation should be explicitly documented.

At minimum configure:

* CPU
* RAM
* disk

The values should be suitable for:

```text
Airflow
+
Airflow DB
+
Oracle
+
CSV Processor
```

---

# 48. Resource Configuration JSON

Consider keeping project resource settings in configuration.

Example:

```json
{
  "resources": {
    "cpu": 8,
    "memory_gb": 12
  }
}
```

This should be used as **development-environment configuration/documentation**, not as a magical mechanism that dynamically controls Docker Desktop resources.

---

# 49. WSL-First Development

All development commands should be executed from WSL.

Example:

```text
WSL
 │
 ├── git
 ├── python
 ├── docker
 ├── airflow CLI
 └── tests
```

Avoid placing the project under:

```text
/mnt/c/...
```

Prefer the Linux filesystem inside WSL, for example:

```text
~/projects/lightweight-airflow-etl
```

This avoids unnecessary Windows filesystem I/O and aligns better with Linux-based containers.

---

# 50. Docker Volumes

Use Docker-managed volumes or WSL-native paths where persistence is required.

Avoid unnecessary bind mounts from:

```text
C:\...
```

into containers.

The objective is to keep the workload primarily within the WSL/Linux environment.

---

# 51. Lightweight Testing

The new project should still have tests, but substantially fewer than the previous platform.

Minimum:

```text
tests/
├── unit/
│   ├── test_config.py
│   ├── test_csv_parser.py
│   ├── test_validator.py
│   └── test_converter.py
│
└── integration/
    ├── test_oracle.py
    └── test_end_to_end.py
```

---

# 52. Unit Tests

Test:

* configuration parsing
* CSV parsing
* type conversion
* date validation
* invalid rows
* valid rows
* chunk processing
* processing result

---

# 53. Oracle Integration Tests

Test against a real Oracle container.

Verify:

```text
CSV
 ↓
Python
 ↓
Oracle
```

and verify the actual resulting rows.

Do not mock Oracle for all tests.

---

# 54. End-to-End Test

Have at least one test representing:

```text
HTTP
 ↓
Airflow DAG
 ↓
config.json
 ↓
file detection
 ↓
CSV processor
 ↓
Oracle
 ↓
VALID / INVALID tables
```

This should become the primary demonstration of the platform.

---

# 55. Performance Test

Create a generated CSV large enough to measure:

* rows/sec
* memory consumption
* Oracle insertion throughput
* processing duration

Compare:

```text
row-by-row
```

against:

```text
bulk/chunked
```

The final implementation should use the efficient approach.

---

# 56. Benchmarking

Record basic metrics:

```text
File size
Rows
Valid rows
Invalid rows
Chunk size
Processing time
Rows/sec
Peak memory
Oracle load time
```

This is particularly important because one of the project's goals is to understand efficient CSV processing.

---

# 57. Minimal CI

Use GitHub Actions, but keep it lightweight.

PR pipeline:

```text
GitHub
  │
  ├── lint
  ├── type check
  ├── unit tests
  └── build/check
```

Oracle integration tests can be added to CI if the container can be reliably started within the workflow.

Do not recreate the previous project's full CI/CD complexity.

---

# 58. Minimal Documentation

Create:

```text
README.md
docs/
├── architecture.md
├── configuration.md
├── csv-engine.md
├── oracle.md
└── development.md
```

The README should allow somebody to go from:

```text
git clone
```

to:

```text
HTTP request
 ↓
DAG
 ↓
CSV
 ↓
Oracle
```

without undocumented manual steps.

---

# 59. Definition of Done

The lightweight platform is complete when:

1. Docker Desktop is running the environment.
2. Development is performed from WSL.
3. Airflow is operational.
4. Oracle Database Free is operational in Docker.
5. Airflow can trigger the DAG through HTTP.
6. DAG uses TaskFlow API.
7. DAG reads `config.json`.
8. DAG determines the expected CSV file.
9. File availability can be waited for efficiently.
10. Deferrable Operator/Trigger has been evaluated and implemented where appropriate.
11. A generated CSV can be produced from the configuration.
12. CSV contains strings, numbers and Oracle-compatible dates.
13. CSV processing is performed by a reusable Python library.
14. CSV processing is chunked/bulk-oriented.
15. The engine does not unnecessarily load the entire file into memory.
16. Configuration defines the expected schema.
17. CSV structure is validated.
18. Data types are validated.
19. Invalid dates are detected.
20. Valid rows are loaded into Oracle.
21. Invalid rows are loaded into a separate Oracle table.
22. Invalid rows contain useful error information.
23. Processing statistics are returned to Airflow.
24. Duplicate processing of the same file is prevented.
25. Python code uses typing.
26. Python code has logging.
27. Python code has meaningful error handling.
28. Unit tests exist.
29. Oracle integration tests exist.
30. At least one end-to-end test exists.
31. A performance test demonstrates bulk processing.
32. CPU/RAM requirements are documented/configurable.
33. The complete environment can be started from WSL.
34. The entire workflow can be demonstrated with one HTTP request.

---

# 60. Main Design Principle

The central purpose of this project is **not to build another production platform**.

It is to answer a much more focused engineering question:

> **How should we build a clean, efficient, reusable Python CSV processing engine and use Airflow to orchestrate it against Oracle?**

Therefore the project should deliberately emphasize:

```text
             AIRFLOW
                │
        orchestration only
                │
                ▼
       ┌─────────────────┐
       │ CSV ENGINE      │
       │                 │
       │ Parse           │
       │ Validate        │
       │ Convert         │
       │ Chunk           │
       │ Bulk Load       │
       └────────┬────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
   VALID ROWS       INVALID ROWS
        │                │
        └───────┬────────┘
                ▼
        ORACLE DATABASE
```

The previous **95-point platform should serve as architectural experience**, but this project should **not reproduce its complexity**. The new implementation should be small, understandable, fast to iterate on, and focused heavily on the CSV engine itself.

[1]: https://www.oracle.com/database/free/?utm_source=chatgpt.com "Oracle AI Database Free | Oracle"
[2]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html "Deferrable Operators & Triggers — Airflow 3.3.1 Documentation"
[3]: https://github.com/oracle/docker-images "GitHub - oracle/docker-images: Official source of container configurations, images, and examples for Oracle products and projects · GitHub"
