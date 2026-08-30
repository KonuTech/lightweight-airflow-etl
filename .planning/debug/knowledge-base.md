# GSD Debug Knowledge Base

Resolved debug sessions. Used by `gsd-debugger` to surface known-pattern hypotheses at the start of new investigations.

---

## airflow-ui-logs-not-visible — Airflow UI task logs unreadable: secret_key mismatch, then orphaned by container recycle
- **Date:** 2026-08-30
- **Error patterns:** secret_key, [api] section, log message source details, NameResolutionError, HTTPConnectionPool, Failed to resolve, could not read served logs, task instance logs blank, airflow ui logs not visible
- **Root cause(s):** AIRFLOW__API__SECRET_KEY was never set in docker-compose.yml's shared x-airflow-common env block, so each Airflow component (apiserver, scheduler, dag-processor, triggerer) minted its own random [api] secret_key at startup, causing every cross-component log-fetch signature check to fail; x-airflow-common also had no shared/persistent volume for /opt/airflow/logs, so each component's task logs lived only on its own ephemeral container filesystem and the UI's fallback HTTP fetch (keyed on the container's own short-ID hostname) permanently orphaned logs the instant that container was recreated
- **Fix:** Added AIRFLOW__API__SECRET_KEY to the shared x-airflow-common env block (same YAML-anchor pattern as the existing AIRFLOW__API_AUTH__JWT_SECRET fix); added a shared named Docker volume airflow-logs:/opt/airflow/logs to x-airflow-common.volumes (inherited by all 5 Airflow services) plus its top-level volume declaration
- **Files changed:** docker-compose.yml
- **Why not caught:** No gate existed for this class. No automated test or CI check exercises docker-compose.yml's Airflow inter-component config (shared secret keys, shared volumes) or simulates a container recycle; this gap only surfaces via manual UI interaction after a task has actually run. The structurally identical Phase 5 fix for AIRFLOW__API_AUTH__JWT_SECRET did not include a completeness check (e.g. "every [api]/[core] secret documented in Airflow's config reference is set and shared") that would have caught its sibling AIRFLOW__API__SECRET_KEY gap at the same time.
- **Recurrence guard:** Inline root-cause comments co-located with both config lines in docker-compose.yml (AIRFLOW__API__SECRET_KEY sits directly beside AIRFLOW__API_AUTH__JWT_SECRET with a comment naming the shared root-cause shape; the airflow-logs volume mount carries a comment naming the container-recycle failure mode) plus this knowledge-base entry, so a future Phase-0 semantic/keyword recall on "secret_key" / "NameResolutionError" / "logs not visible" surfaces the prior fix before re-diagnosing from scratch. No automated regression test is possible for this class in this project (no test suite covers docker-compose.yml); the KB entry plus the in-file comments are the strongest available guard.
---
