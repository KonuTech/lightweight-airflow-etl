.PHONY: up down reset logs

up:               ## Start the full stack (Airflow + Oracle)
	docker compose up -d --wait

down:              ## Stop containers only -- named volumes (D-13) stay intact
	docker compose down

reset:             ## Full wipe: stop containers AND remove volumes (D-15)
	docker compose down -v

logs:              ## Tail logs from every service
	docker compose logs -f

# Later phases (2-6) add targets here (make test, make lint, make verify, make benchmark)
# rather than inventing separate tooling -- this Makefile is the project-wide command
# entrypoint (D-14), not scoped to Phase 1 only.
