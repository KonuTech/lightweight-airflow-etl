---
phase: 6
slug: end-to-end-verification-benchmark-ci-docs
status: verified
threats_open: 0
asvs_level: 1
created: 2026-08-30
---

# Phase 6 — Security

> Per-phase security contract: threat register, accepted risks, and audit trail.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| Test process → Airflow REST API | e2e test authenticates with local-dev `admin`/`admin` (INFRA-03), holds a JWT bearer token for the test's duration | JWT bearer token (local-dev-only, non-sensitive) |
| Test process → Oracle | e2e test connects via `csv_processor.load.get_connection()` (env-var-first, `admin`/`admin` local-dev fallback) | Oracle credentials, row data |
| `benchmark/naive_loader.py` → Oracle | Builds a raw SQL `INSERT` string via table/column-name interpolation (Oracle has no bind-parameter mechanism for identifiers) | Table/column identifiers, row data |
| Forked-repo PR → `lint-type-unit`/`oracle-e2e` jobs | Untrusted PR code runs in both jobs; neither job may hold write access or repo secrets | PR source code |
| Third-party GitHub Actions → CI runner | `astral-sh/setup-uv`, `actions/checkout`, `stefanzweifel/git-auto-commit-action` — a compromised/malicious action version could run arbitrary code in the job | Action code execution |
| `push: main` (post-merge, trusted only) → `regenerate-executive-summary` job | Runs only after a PR's required checks already passed and merged — never on untrusted fork PR code | `contents: write` scope |
| Documentation content → reader's own local setup | Docs must never instruct a reader toward a less-secure configuration than this project already ships | Configuration guidance |

---

## Threat Register

| Threat ID | Category | Component | Severity | Disposition | Mitigation | Status |
|-----------|----------|-----------|----------|-------------|------------|--------|
| T-06-01 | Information Disclosure | `scripts/dag_polling.py` / `tests/e2e/test_csv_ingest_e2e.py` | medium | mitigate | JWT bearer token passed only as a function argument/header value, never printed to stdout/stderr/pytest output — verified via grep, no `print`/`logging`/`echo` calls reference `token` | closed |
| T-06-02 | Tampering | `.github/workflows/ci.yml` (`astral-sh/setup-uv`, `actions/checkout`) | high | mitigate | Exact immutable tags pinned (`actions/checkout@v7.0.1`, `astral-sh/setup-uv@v10.0.1`) — verified via grep, no floating `@v7`/`@v10` references anywhere in either workflow file | closed |
| T-06-03 | Elevation of Privilege | `.github/workflows/ci.yml` trigger | high | mitigate | `on: pull_request` only, never `pull_request_target` — verified; no `permissions:` block anywhere in the file (workflow- or job-level) | closed |
| T-06-04 | Tampering (SQL injection via identifier) | `benchmark/naive_loader.py` | high | mitigate | `is_safe_identifier()` (same allowlist `load.insert_rows()` uses) called on `table` and every column name immediately before interpolation — verified via grep, present at both call sites | closed |
| T-06-05 | Denial of Service (CI disk exhaustion) | `oracle-e2e` job's `docker compose up` | low | accept | Accepted per Pitfall 5's own guidance — `slim-faststart` Oracle tag documented as fallback rather than pre-emptively switched; watch-for-it risk, not mitigated | closed |
| T-06-06 | Repudiation | Branch Protection status-check enforcement | high | mitigate | Documented as an outstanding manual step in `docs/development.md` (§"Repo Settings → Branches → Branch protection rules") rather than silently assumed satisfied by the workflow YAML alone — verified present; actual GitHub configuration remains a human/repo-admin action tracked in `06-UAT.md` item 2 | closed |
| T-06-07 | Tampering (infinite CI-trigger loop) | `.github/workflows/readme-summary.yml` auto-commit step | medium | mitigate | Default `GITHUB_TOKEN` used (never a PAT) for `stefanzweifel/git-auto-commit-action@v7.2.0` — verified via grep; a `GITHUB_TOKEN`-authored commit structurally does not re-trigger push-based workflows | closed |
| T-06-08 | Elevation of Privilege | `permissions: contents: write` scope | high | mitigate | Scoped exclusively to the `regenerate-executive-summary` job block (not workflow-level) — verified; the file has only one job, so no risk of an unrelated job inheriting write access | closed |
| T-06-09 | Tampering | `stefanzweifel/git-auto-commit-action` (third-party action) | high | mitigate | Pinned to exact tag `v7.2.0` — verified via grep, no floating major-version reference | closed |
| T-06-10 | Information Disclosure (documentation-driven misconfiguration) | `docs/*.md`, README.md | low | accept | Docs only restate this project's own already-secure, already-documented defaults (127.0.0.1-only port binds, local-dev-only `admin`/`admin`) — no new configuration guidance widens the attack surface | closed |

*Status: open · closed · open — below high threshold (non-blocking)*
*Severity: critical > high > medium > low — only open threats at or above `workflow.security_block_on` (high) count toward `threats_open`*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

| Risk ID | Threat Ref | Rationale | Accepted By | Date |
|---------|------------|-----------|-------------|------|
| R-06-01 | T-06-05 | CI runner disk headroom for the ~14.8GB Oracle+Airflow image/volume footprint is unconfirmed against a live GitHub-hosted runner this session — accepted as a "watch for it" risk per 06-RESEARCH.md's Pitfall 5, with `gvenzl/oracle-free:23.26.2-slim-faststart` documented as a fallback tag rather than pre-emptively switched | Phase 6 planning (06-CONTEXT.md, 06-RESEARCH.md) | 2026-08-29 |
| R-06-02 | T-06-10 | Documentation is purely descriptive of this project's own already-reviewed secure defaults; no new configuration guidance introduced | Phase 6 planning (06-CONTEXT.md) | 2026-08-29 |

*Accepted risks do not resurface in future audit runs.*

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-08-30 | 10 | 10 | 0 | Orchestrator (grep-level L1 verification, ASVS L1 short-circuit — no auditor spawn required per `register_authored_at_plan_time: true` + `threats_open: 0` + `asvs_level == 1`) |

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-08-30
