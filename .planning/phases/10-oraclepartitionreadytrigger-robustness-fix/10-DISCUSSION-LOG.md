# Phase 10: `OraclePartitionReadyTrigger` Robustness Fix - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-09-02
**Phase:** 10-`OraclePartitionReadyTrigger` Robustness Fix
**Areas discussed:** Exception scope, exception specificity, retry cap shape, exhausted-retry
signal, backoff strategy, finally-block close-failure handling, logging level for retries, test
coverage shape

**Mode:** `--auto` — fully autonomous, no user interaction. All 8 gray areas auto-selected and
resolved with Claude's recommended defaults in a single pass, per the `--auto` contract.

---

## Exception scope

| Option | Description | Selected |
|--------|-------------|----------|
| Inside, covering the full poll cycle | connect_async() + cursor/execute/fetch all inside one try block | ✓ |
| Outside (current, broken state) | Only cursor/execute/fetch wrapped, connect_async() unguarded | |

**Auto-selected:** Inside, covering full poll cycle. **Rationale:** Pitfall 9 (research)
identified the current code leaves `connect_async()` outside `try` entirely — the single most
likely transient-failure point is completely unhandled today.

---

## Exception specificity

| Option | Description | Selected |
|--------|-------------|----------|
| Narrow OperationalError only | Catch only the connection/network-level exception subclass | ✓ |
| Broad DatabaseError/Error | Catch any database error for retry | |

**Auto-selected:** Narrow OperationalError only. **Rationale:** Matches PITFALLS.md's Technical
Debt Patterns table's explicit tradeoff call: "narrow-and-correct is the right tradeoff... widen
only if a specific, evidenced transient error is observed escaping this net in practice."

---

## Retry cap shape

| Option | Description | Selected |
|--------|-------------|----------|
| Count-based, max 10 consecutive failures | Simple, deterministic, testable without wall-clock mocking | ✓ |
| Elapsed-time-based | e.g. give up after M total minutes | |

**Auto-selected:** Count-based, max 10 consecutive failures (~5 min at default poke_interval=30s).

---

## Exhausted-retry signal

| Option | Description | Selected |
|--------|-------------|----------|
| Re-raise last exception | Reuses Airflow's existing uncaught-exception-fails-the-task handling | ✓ |
| Custom failure TriggerEvent | New signaling shape, needs execute_complete() to interpret it | |

**Auto-selected:** Re-raise last exception. **Rationale:** Consistent with the non-retryable-error
path (D-02) already relying on the same "let it propagate" mechanism — avoids introducing a second
failure-signaling shape for no added clarity.

---

## Backoff strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Exponential backoff capped at poke_interval, matching verify_environment.py | Reuses established in-repo pattern | ✓ |
| Fixed poke_interval sleep between all retries | Simpler but no backoff curve | |

**Auto-selected:** Exponential backoff (base 1.0s, `base * 2**(attempt-1)`, capped at
`poke_interval`=30.0s), matching `scripts/verify_environment.py`'s existing
`AUTH_RETRY_ATTEMPTS`/`AUTH_RETRY_BASE_DELAY_SECONDS` convention.

---

## finally-block close-failure handling

| Option | Description | Selected |
|--------|-------------|----------|
| Guard with nested try/except, log at debug, never re-raise | Exact Pitfall 10 fix | ✓ |
| Leave connection.close() unconditional | Current, broken state — can mask the original exception | |

**Auto-selected:** Guard with nested try/except. **Rationale:** Exact match to PITFALLS.md's
Pitfall 10 documented fix — same code block as the other decisions, implemented together.

---

## Logging level for retries

| Option | Description | Selected |
|--------|-------------|----------|
| warning | A retry deserves operator attention even before it becomes a failure | ✓ |
| info | Lower-visibility logging | |

**Auto-selected:** warning.

---

## Test coverage shape

| Option | Description | Selected |
|--------|-------------|----------|
| Extend tests/unit/test_oracle_partition_trigger.py | Existing file already has the needed AsyncMock scaffolding | ✓ |
| New separate test file | Would duplicate existing mocking setup | |

**Auto-selected:** Extend existing file. Four new test cases specified in CONTEXT.md D-08.

---

## Claude's Discretion

- Exact retry-count/backoff constant names and placement.
- Whether the retry-attempt counter resets to 0 after any single successful poll cycle (recommended:
  yes, per D-03's "10 consecutive failures" wording).

## Deferred Ideas

None — discussion stayed within Phase 10's domain.
