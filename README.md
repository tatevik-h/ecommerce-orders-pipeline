# E-commerce Orders Pipeline

An hourly batch pipeline for e-commerce order events, orchestrated with **Apache Airflow 3**, stored in **PostgreSQL** and modelled with **dbt**. It focuses on the parts that matter in production: **data quality**, **idempotent reruns and backfills**, **incremental processing** and **monitoring**.

Each run processes one hour of order events (placed → paid → shipped → delivered, plus cancellations and refunds). It validates every event, quarantines bad ones with the reason, upserts the latest status of every order, builds and tests the analytics models, and checks whether the hour's volume looks normal.

## Architecture

```mermaid
flowchart LR
    SRC[Order events source<br/>hourly window] -->|extract_to_raw| RAW[(raw.order_events<br/>JSONB, as received)]
    RAW -->|validate_and_load| DQ{Validation rules}
    DQ -->|valid, de-duplicated| STG[(staging.order_events)]
    DQ -->|failed + reasons| REJ[(quality.rejected_events)]
    DQ --> MET[(quality.batch_metrics)]
    STG -->|upsert_current_orders| ORD[(staging.orders<br/>latest status)]
    STG --> DBT[[dbt build + tests]]
    ORD --> DBT
    DBT --> FCT[(marts.fct_orders<br/>incremental merge)]
    FCT --> AGG[(marts.agg_hourly_sales)]
    MET -->|check_volume_anomaly| VOL[(quality.volume_checks)]
```

### DAG: `orders_hourly_pipeline`

| Task | What it does |
|---|---|
| `ensure_schema` | Creates schemas and tables if missing (idempotent DDL) |
| `extract_to_raw` | Pulls the hour's events and lands them unchanged in `raw` (replaces the batch on rerun) |
| `validate_and_load` | Runs data quality rules, quarantines failures, upserts valid events, records batch metrics, all in one transaction |
| `upsert_current_orders` | Applies status changes to `staging.orders`; older replayed events can never overwrite a newer status |
| `dbt_source_freshness` | Fails if staging data is stale (warn after 2h, error after 6h) |
| `dbt_build` | Loads seeds, builds the intermediate and mart models and runs all dbt tests |
| `check_volume_anomaly` | Compares the hour's volume with the same hour on the previous 7 days (z-score) |
| `apply_retention` | Deletes raw landing data older than `RAW_RETENTION_DAYS` (default 30) |

The schedule uses `CronDataIntervalTimetable`, so every run covers a full hour `[start, end)`. `max_active_runs=1` keeps status upserts in order, and all tasks retry with exponential backoff.

## Data quality

**Row-level rules** (`src/orders_pipeline/validation.py`), run on every event:
- required fields present and not null
- accepted values for `status` and `currency`
- amount numeric and in range (`0 < amount ≤ 100,000`)
- two-letter uppercase country code
- timezone-aware timestamp inside the batch window
- duplicate `event_id` within the batch (at-least-once delivery)

Failed events go to `quality.rejected_events` with **fixed reason codes**, so rejections can be counted and monitored per rule. Nothing is dropped silently, and every batch's in/valid/rejected/duplicate counts are stored in `quality.batch_metrics`.

**Statistical check** (`src/orders_pipeline/anomaly.py`): some problems pass every rule, for example an upstream outage that delivers half the data. The volume check compares the hour with the same hour on previous days and flags deviations above 3 standard deviations. Results are stored in `quality.volume_checks`. Set the `fail_on_anomaly` param to make the run fail and alert.

**dbt tests**: `unique`, `not_null`, `accepted_values` and `relationships` (to the FX seed), a custom generic test for unique column combinations, a singular test that every order's lifecycle is chronological, and source freshness.

Trigger a run with the `volume_multiplier` param set to `0.5` to simulate an outage and watch the anomaly check catch it.

## Idempotency and backfills

- Every write is scoped to the batch's `batch_start`. Raw and rejected rows for a batch are replaced, and staging rows are upserted on their natural keys.
- `staging.orders` upserts have a guard (`WHERE last_event_ts <= EXCLUDED.last_event_ts`), so replaying an old hour can't roll an order back to an earlier status.
- The source is deterministic per window, like re-reading a partition, so reruns produce the same data.
- `fct_orders` is an **incremental dbt model** (`merge` on `order_id`) that only re-processes orders with new or changed events.

Rerunning or backfilling any range leaves the warehouse in the same state. This is covered by the integration tests.

```bash
make backfill FROM=2026-11-01T00:00:00 TO=2026-11-02T00:00:00
```

## Run it locally

Requirements: Docker with Compose.

```bash
cp .env.example .env
docker compose up -d --build        # or: make up
```

Open http://localhost:8080 (login `airflow` / `airflow`), unpause `orders_hourly_pipeline`, and trigger a run. The warehouse is exposed on `localhost:5432` (database `warehouse`, user `warehouse`, password `warehouse`):

```sql
select * from quality.batch_metrics order by batch_start desc limit 24;
select reasons[1] as reason, count(*) from quality.rejected_events group by 1 order by 2 desc;
select * from marts.agg_hourly_sales order by order_hour desc, gross_revenue_eur desc limit 20;
```

To load a week of history quickly without Airflow, so the volume check has a baseline:

```bash
pip install psycopg2-binary
make seed
```

## Tests and CI

```bash
pip install -r requirements-dev.txt \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt
pytest -q                                            # unit + DAG integrity tests
WAREHOUSE_TEST_DSN=postgresql://warehouse:warehouse@localhost:5432/warehouse_test pytest -q   # + integration
```

- **Unit tests**: generator determinism, every validation rule, anomaly detection edge cases
- **Integration tests** (real Postgres): rerun idempotency, out-of-order backfills, metrics and retention
- **DAG integrity**: the DAG imports cleanly, task order, retries

GitHub Actions runs lint (ruff), all tests against a Postgres service container, loads 4 days of batches, and runs `dbt build` and `dbt source freshness`.

## Project layout

```
dags/orders_hourly_pipeline.py      Airflow 3 DAG (TaskFlow API)
src/orders_pipeline/
    generator.py                    deterministic synthetic source with injected bad data
    validation.py                   row-level data quality rules
    anomaly.py                      z-score volume check
    loading.py                      idempotent warehouse I/O
    sql/warehouse_schema.sql        raw / staging / quality schemas
dbt/                                intermediate + mart models, seeds, tests
scripts/run_batches.py              run batches without Airflow (local seeding, CI)
tests/                              pytest suite
docker-compose.yaml, Dockerfile     local Airflow + Postgres stack
```

## Design decisions

- **Only small metadata goes through XCom.** Tasks exchange data through the warehouse, never through XCom.
- **Business logic lives in a plain Python package** (`src/orders_pipeline`) that's tested without Airflow. The DAG only wires steps together.
- **dbt runs in its own virtualenv** inside the image, so its dependencies never conflict with Airflow's.
- **The raw layer keeps events exactly as received** (JSONB), so any batch can be re-validated after a rule changes.

## Roadmap

- Streaming source: consume events from Kafka instead of the synthetic generator
- Great Expectations checkpoint as an additional validation step
- Native Postgres partitioning of `raw` and `staging` by day
- Cloud version: GCS + BigQuery, deployed on Cloud Composer
