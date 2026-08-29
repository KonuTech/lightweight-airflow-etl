.PHONY: up down reset logs verify smoke-test generate fixtures fixtures-verify verify-phase2 verify-phase3 verify-phase4

up:               ## Start the full stack (Airflow + Oracle)
	docker compose up -d --wait

down:              ## Stop containers only -- named volumes (D-13) stay intact
	docker compose down

reset:             ## Full wipe: stop containers AND remove volumes (D-15)
	docker compose down -v

logs:              ## Tail logs from every service
	docker compose logs -f

verify:            ## Confirm Oracle schema + admin/admin dual-auth against a running stack
	uv run python scripts/verify_environment.py

smoke-test:        ## Cold start: wipe all state, boot fresh, confirm the stack is genuinely alive
	$(MAKE) reset
	$(MAKE) up
	$(MAKE) verify

generate:          ## Generate deterministic business-row CSV fixtures for every dataset (D-16f)
	uv run python generator/generate_csv.py --dataset customers && uv run python generator/generate_csv.py --dataset orders

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

# Later phases (2-6) add targets here (make test, make lint, make benchmark)
# rather than inventing separate tooling -- this Makefile is the project-wide command
# entrypoint (D-14), not scoped to Phase 1 only. Wiring verify-phase2 (or its
# successors) into GitHub Actions CI is explicitly Phase 6's job (CI-01), not
# touched here.
