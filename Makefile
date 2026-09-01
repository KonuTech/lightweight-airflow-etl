.PHONY: up down reset destroy rebuild logs verify smoke-test generate fixtures fixtures-verify verify-phase2 verify-phase3 verify-phase4 verify-phase5 benchmark lint verify-evidence verify-phase6 verify-phase7 verify-phase8

up:               ## Start the full stack (Airflow + Oracle)
	docker compose up -d --wait

down:              ## Stop containers only -- named volumes (D-13) stay intact
	docker compose down

reset:             ## Full wipe: stop containers AND remove volumes (D-15)
	docker compose down -v

destroy:           ## Deepest teardown: reset (D-15) PLUS remove locally-built images and orphan containers
	docker compose down -v --rmi local --remove-orphans

rebuild:           ## Rebuild locally-built images from scratch (no cache), then start the stack fresh
	docker compose build --no-cache
	$(MAKE) up

logs:              ## Tail logs from every service
	docker compose logs -f

verify:            ## Confirm Oracle schema + admin/admin dual-auth against a running stack
	uv run python scripts/verify_environment.py

smoke-test:        ## Cold start: wipe all state, boot fresh, confirm the stack is genuinely alive
	$(MAKE) reset
	$(MAKE) up
	$(MAKE) verify

generate:          ## Generate correlated customers+orders CSV fixtures via one combined invocation (D-21/D-22)
	uv run python generator/generate_csv.py --correlated

fixtures:          ## Materialize the byte-level fixture corpus + (re)write its digest oracle (D-16f)
	uv run python -m tools.corpus generate --manifest tests/fixtures/corpus.yaml --out tests/fixtures/csv --write-digests tests/fixtures/CORPUS.sha256

fixtures-verify:   ## Regenerate the corpus to a temp dir and diff SHA-256 against the committed oracle (D-16f)
	uv run python -m tools.corpus verify --manifest tests/fixtures/corpus.yaml --digests tests/fixtures/CORPUS.sha256

verify-phase2:     ## Phase 2's own combined local gate: full unit suite + fixture digest-oracle verification (D-16g)
	uv run pytest tests/unit/ -x
	$(MAKE) fixtures-verify

# Phase 3 introduces no new fixture-digest mechanism beyond Phase 2's
# already-committed tests/fixtures/CORPUS.sha256, so verify-phase3 follows
# verify-phase2's exact shape (a plain full-suite run) with no added
# fixtures-verify step.
verify-phase3:     ## Phase 3's own combined local gate: full unit suite covering detect/compression/structural/type/nullability/chunking (TEST-01)
	uv run pytest tests/unit/ -x

# Phase 4 is this project's first phase-gate that genuinely needs a running Oracle
# container (`make up` first) -- unlike verify-phase2/verify-phase3, which never touch
# Oracle, verify-phase4 runs BOTH the unit suite and the real-Oracle integration suite
# (LOAD-01..04, ENGINE-08, TEST-02).
verify-phase4:     ## Phase 4's own combined local gate: unit + real-Oracle integration suites (requires `make up` first)
	uv run pytest tests/unit/ -x
	uv run pytest tests/integration/ -x

# Phase 5 needs a running Airflow container to structurally validate the DAG --
# requires `make up` first, same as verify-phase4. The DagBag structure check
# uses BundleDagBag (not the plain airflow.models.DagBag) -- 05-01-SUMMARY.md's
# own recorded deviation found that plain DagBag never adds the dags folder to
# sys.path, so csv_ingest.py's `from _common import paths, reporting` fails
# under it even though it imports cleanly under Airflow's real dag-processor.
verify-phase5:     ## Phase 5's own combined local gate: unit suite + live DagBag structure check (requires `make up` first)
	uv run pytest tests/unit/ -x
	docker compose exec -T airflow-scheduler python -c "\
from pathlib import Path; \
from airflow.dag_processing.dagbag import BundleDagBag; \
b = BundleDagBag(bundle_path=Path('/opt/airflow/dags'), dag_folder='/opt/airflow/dags'); \
assert not b.import_errors, b.import_errors; \
dag = b.dags['csv_ingest']; \
required = {'load_config_task','wait_for_file','process_csv_task','load_results_task','report_result_task'}; \
assert required.issubset(set(dag.task_ids)), dag.task_ids; \
assert dag.get_task('wait_for_file').deferrable is True; \
print('DAGBAG_OK')"

# Later phases (2-6) add targets here (make test, make lint, make benchmark)
# rather than inventing separate tooling -- this Makefile is the project-wide command
# entrypoint (D-14), not scoped to Phase 1 only. Wiring verify-phase2 (or its
# successors) into GitHub Actions CI is explicitly Phase 6's job (CI-01), not
# touched here.

benchmark:         ## Naive-vs-bulk Oracle write comparison at ~100K rows (TEST-04, requires `make up` first)
	uv run python -m benchmark.run_benchmark --mode bulk --rows 100000

lint:              ## Whole-repo lint + format-check + type-check (D-14, CI-01's lint-type-unit job)
	uv run ruff check . && uv run ruff format --check . && uv run mypy .

verify-evidence:   ## Reproducible Oracle evidence capture: latest ingestion + customers x orders business report (D-09/D-10, requires `make up` first)
	docker compose exec -T oracle sqlplus -s admin/admin@//localhost:1521/FREEPDB1 < scripts/verify_evidence.sql

# Phase 6's own combined local gate -- mirrors verify-phase4/verify-phase5's exact
# shape (unit suite first, then phase-specific live checks), requires `make up` first.
verify-phase6:     ## Phase 6's own combined local gate: unit + e2e suites, lint, and evidence verification (requires `make up` first)
	uv run pytest tests/unit/ -x
	uv run pytest tests/e2e/ -x
	$(MAKE) lint
	$(MAKE) verify-evidence

# Phase 7's own combined local gate -- mirrors verify-phase6's exact shape (unit ->
# e2e -> lint -> verify-evidence), requires `make up` first. Phase 7 adds one step
# verify-phase6 didn't have: the integration suite, since
# test_correlation_constraints.py (Plan 07-04's DDL/trigger coverage) lives under
# tests/integration/, not tests/unit/ or tests/e2e/.
verify-phase7:     ## Phase 7's own combined local gate: unit + e2e + integration suites, lint, and evidence verification (requires `make up` first)
	uv run pytest tests/unit/ -x
	uv run pytest tests/e2e/ -x
	uv run pytest tests/integration/ -x
	$(MAKE) lint
	$(MAKE) verify-evidence

# Phase 8 introduces no new pytest-testable logic (compose/Dockerfile/docs wiring
# only, per 08-RESEARCH.md) -- verify-phase8 is a thin wrapper around the now-extended
# scripts/verify_environment.py (verify_generator_importable()/verify_data_write_access(),
# ENV-01/ENV-02), requires `make up` first.
verify-phase8:     ## Phase 8's own combined local gate: container-exec import + data write-access checks (requires `make up` first)
	uv run python scripts/verify_environment.py
