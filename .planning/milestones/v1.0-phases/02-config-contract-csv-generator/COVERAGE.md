# Phase 2 — API Coverage Declaration

No external API integration: the detector's `detected: true` fired on the word "REST" inside
`02-01-PLAN.md`'s threat-model note about a *future* phase (Phase 5's Airflow REST-trigger runtime
`conf`, flagged forward as a canon path-traversal consideration, not something Phase 2 builds or
calls). Phase 2 (config contract + CSV generator + fixture corpus) makes no HTTP/SDK/API calls of
any kind — `csv_processor.config.loader.load_config()` reads local JSON files, `generator/generate_csv.py`
writes local CSV files via Faker + stdlib `csv`, and `tools/corpus/` reads a local YAML manifest and
writes local fixture files via stdlib `gzip`/`zipfile`/`hashlib`. No external API/SDK/service
integration exists in this phase's scope; Oracle access and Airflow's own REST API belong to later
phases (4 and 5 respectively).
