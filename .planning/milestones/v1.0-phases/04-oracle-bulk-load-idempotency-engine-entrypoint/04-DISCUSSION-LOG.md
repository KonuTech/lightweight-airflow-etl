# Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-29
**Phase:** 4-Oracle Bulk Load, Idempotency & Engine Entrypoint
**Areas discussed:** Re-run/idempotency behavior, Per-file load atomicity, Oracle bulk-insert batch size

> This phase ran in `--auto` mode: no interactive `AskUserQuestion` prompts were shown. For each
> gray area, the recommended option (the one most consistent with `.planning/research/
> ARCHITECTURE.md`'s already-settled design) was auto-selected without user input.

---

## Re-run / idempotency behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Return the original recorded outcome (status + counts from `ingestion_metadata`) | Reuses the existing closed status enum; no new consumer-facing status value | ✓ |
| Add a new distinct status (e.g. `ALREADY_PROCESSED`/`SKIPPED`) | More explicit signal, but expands the enum every consumer must handle | |

**Selected:** Reuse the original recorded outcome, no new status.
**Notes:** [auto] Recommended default — ENGINE-08/REQUIREMENTS.md's status enum is already closed at 7 members; adding an 8th is a new capability decision better made explicitly later if a real need surfaces, not assumed here.

---

## Per-file load atomicity

| Option | Description | Selected |
|--------|-------------|----------|
| All-or-nothing per file (single transaction: valid rows + invalid rows + metadata upsert) | Simple, matches "one connection per `process()` call" design; a failed load leaves nothing behind to clean up | ✓ |
| Partial-commit-with-resume (chunk-level progress tracking) | More resilient to mid-load failures on very large files, but needs a new metadata schema and resume logic | |

**Selected:** All-or-nothing per file.
**Notes:** [auto] Recommended default — matches ARCHITECTURE.md's stated design; partial-commit-with-resume is real added complexity not currently requested.

---

## Oracle bulk-insert batch size

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse each dataset's existing `chunk_size` config (5000) as the `executemany()` array size | No new config key; one fewer tunable to keep in sync | ✓ |
| Introduce a separate Oracle-specific batch-size config key | More tuning flexibility if Oracle's own sweet spot differs from the CSV-side chunk size | |

**Selected:** Reuse `chunk_size`.
**Notes:** [auto] Recommended default — avoids a redundant config knob; can be split later without breaking this decision if Oracle-specific tuning proves necessary.

---

## Claude's Discretion

- Exact SQL/parameter-binding order for `executemany()` calls against `<DATASET>_VALID`/`<DATASET>_INVALID`.
- Exact sequencing of "compute checksum → check `ingestion_metadata` → short-circuit or load → upsert metadata" within one transaction.
- Which Oracle exception types map to `DATABASE_ERROR` vs `PROCESSING_ERROR` — flagged for a research pass (`setinputsizes()` type-derivation, `batcherrors` semantics per STATE.md's Blockers/Concerns).

## Deferred Ideas

None — discussion stayed within phase scope.
