.PHONY: up down reset logs verify smoke-test generate

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

# Later phases (2-6) add targets here (make test, make lint, make benchmark)
# rather than inventing separate tooling -- this Makefile is the project-wide command
# entrypoint (D-14), not scoped to Phase 1 only.
