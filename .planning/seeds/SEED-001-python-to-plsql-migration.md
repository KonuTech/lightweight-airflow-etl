---
id: SEED-001
status: dormant
planted: 2026-08-30
planted_during: Phase 07 (Correlated Customer-Order Business Report)
trigger_when: a future phase needs data-processing logic that's awkward or slow to express in the Python csv_processor engine, or Oracle-side performance/complexity pressure makes PL/SQL clearly the better fit
scope: unknown
---

# SEED-001: Consider moving more data-processing logic from Python to Oracle PL/SQL

## Why This Matters

Raised during Phase 7's discuss-phase (2026-08-30) as a future architectural direction. Moving
more data-processing logic (validation, normalization, correlation) from the Python
`csv_processor` engine into Oracle PL/SQL would reverse this project's currently shipped "thin
Python engine, dumb Oracle sink" design — see `PROJECT.md`'s "What This Is" and Core Value
sections. Not something to act on casually: it's a real architectural reversal, not an add-on.

## When to Surface

**Trigger:** a future phase needs data-processing logic that's awkward or slow to express in the
Python `csv_processor` engine, or Oracle-side performance/complexity pressure makes PL/SQL
clearly the better fit.

This seed will surface during `/gsd-new-milestone` when the milestone scope matches.

## Scope Estimate

**Unknown** — run `/gsd-capture --seed --enrich SEED-001` to estimate effort. Given it reverses a
shipped design decision, treat as at least milestone-sized until scoped.

## Breadcrumbs

- `.planning/PROJECT.md` — "What This Is" / Core Value (the "thin Python engine" design this would
  reverse) and Key Decisions table (two-tier reuse decision, validation-scope decision)
- `.planning/phases/07-correlated-customer-order-business-report/07-CONTEXT.md` — Deferred Ideas
  (original discussion context)
- `packages/csv-processor/src/csv_processor/` — the Python engine this idea would partially
  displace

## Notes

Captured via one-shot seed capture at Phase 7 close, per explicit user choice ("capture only, no
research or decision now") over exploring it immediately or dropping it.
