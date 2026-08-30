# Project Retrospective

## Milestone: v1.0 — MVP

**Shipped:** 2026-08-30
**Phases:** 7 | **Plans:** 36 | **Tasks:** ~89

### What Was Built

A local Airflow + Oracle CSV ETL platform: config-driven generator → deterministic CSV fixtures →
HTTP-triggered `csv_ingest` DAG with a deferrable file-wait → Airflow-agnostic `csv_processor`
engine (detect/parse/validate/normalize/chunk) → Oracle bulk-load with checksum idempotency →
a `report_ready` DAG proving a live, correlated `customers ⋈ orders` business report → CI
(lint/type/unit + a real Oracle+Airflow e2e job) and a self-updating README Executive Summary.

### What Worked

- Two-tier reuse of the sibling `airflow-platform` repo (vendor pure detection code, reimplement
  pipeline-coupled logic) kept the engine genuinely Airflow-agnostic from day one
- "Never trust exit status — verify via the DB's own metadata views" discipline (Phase 1) caught
  real gaps early and became a standing project habit
- Live-verification-over-assertion as a hard rule: nearly every phase's Success Criteria required
  a real command run against the live stack, not code-reading — this surfaced Phase 6's
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` bug and Phase 7's correlation gap, both invisible
  to a warm dev stack

### What Was Inefficient

- Phase 7's post-completion CI work (fixing `readme-summary.yml`) took multiple iterations because
  each fix uncovered a new GitHub Actions platform constraint (branch-name mismatch → required-checks
  vs. direct push → GITHUB_TOKEN anti-recursion → GITHUB_TOKEN can't dispatch workflows) — a single
  upfront read of GitHub's Actions security model would have surfaced all four at once
- A pasted PAT was briefly exposed in a terminal command mid-session — a reminder to always route
  secrets through non-echoing stdin, never a `!`-prefixed command line

### Patterns Established

- Every business-report SQL text lives in exactly one place (`scripts/verify_evidence.sql`) and is
  mirrored verbatim everywhere else — never re-derived independently
- Config `precision`/`scale` fields are generator-side metadata only, never enforced by the
  validation engine — schema width just needs to be *at least* as wide, not identical
- CI auto-commits land via a PR + scoped PAT + auto-merge, never a direct push to a protected branch

### Key Lessons

- Branch protection's required-status-checks apply to every ref update, including direct pushes —
  a bot with only `GITHUB_TOKEN` can never satisfy them without going through a real PR
- GitHub's anti-recursion rule (GITHUB_TOKEN-authored events don't trigger further workflow runs)
  applies to PRs a bot opens, not just pushes — plan CI automation around this from the start
- A demo/report generator's realism (row counts, value ranges, date spread) is a first-class
  design concern, not an afterthought — cardinality mismatches silently degrade "evidence" quality

### Cost Observations

- Sessions: 1 extended session covering all 7 phases plus post-ship CI hardening and documentation
- Notable: the CI redesign (3 rounds of platform-constraint discovery) was the single most
  iteration-heavy piece of work in the milestone, disproportionate to its actual code footprint

## Cross-Milestone Trends

_First milestone — no cross-milestone data yet._
