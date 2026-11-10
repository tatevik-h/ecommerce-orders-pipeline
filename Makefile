.PHONY: up down logs test lint format seed dbt-build backfill

AIRFLOW_EXEC = docker compose exec airflow-scheduler

up:            ## Build and start Airflow + Postgres
	@test -f .env || cp .env.example .env
	docker compose up -d --build

down:          ## Stop the stack (keeps the Postgres volume)
	docker compose down

logs:          ## Follow scheduler logs
	docker compose logs -f airflow-scheduler

test:          ## Unit tests (+ integration tests if WAREHOUSE_TEST_DSN is set)
	pytest -q

lint:          ## Lint and check formatting
	ruff check . && ruff format --check .

format:
	ruff format . && ruff check --fix .

seed:          ## Load 7 days of history without Airflow (warehouse must be reachable on localhost)
	PYTHONPATH=src python scripts/run_batches.py --start 2026-11-01T00:00:00+00:00 --hours 168

dbt-build:     ## Build and test dbt models inside the stack
	$(AIRFLOW_EXEC) bash -c '$$DBT_BIN build --project-dir $$DBT_PROJECT_DIR --profiles-dir $$DBT_PROJECT_DIR --target-path /tmp/dbt_target'

backfill:      ## Backfill a range: make backfill FROM=2026-11-01T00:00:00 TO=2026-11-02T00:00:00
	$(AIRFLOW_EXEC) airflow backfill create --dag-id orders_hourly_pipeline \
		--from-date $(FROM) --to-date $(TO) --max-active-runs 1
