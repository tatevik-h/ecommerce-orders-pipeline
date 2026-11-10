"""
### Hourly e-commerce orders pipeline

Processes one hour of order events per run:

1. **ensure_schema**: create warehouse schemas and tables if they're missing
2. **extract_to_raw**: pull the hour's events from the source and land them unchanged in `raw`
3. **validate_and_load**: run data quality rules, quarantine bad events in `quality.rejected_events`,
   upsert valid events into `staging.order_events` and record batch metrics
4. **upsert_current_orders**: apply status changes to `staging.orders` (latest status per order)
5. **dbt_source_freshness** / **dbt_build**: check freshness, then build and test the dbt models in `marts`
6. **check_volume_anomaly**: compare the batch volume with the same hour on previous days (z-score)
7. **apply_retention**: delete raw landing data older than the retention period

Each run only touches its own data interval, so retries and backfills are idempotent.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, chain, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowException
from airflow.timetables.interval import CronDataIntervalTimetable

from orders_pipeline import loading
from orders_pipeline.anomaly import detect_volume_anomaly
from orders_pipeline.generator import generate_events
from orders_pipeline.validation import validate_batch

log = logging.getLogger(__name__)

WAREHOUSE_CONN_ID = os.environ.get("WAREHOUSE_CONN_ID", "warehouse_db")
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
RAW_RETENTION_DAYS = int(os.environ.get("RAW_RETENTION_DAYS", "30"))
DBT_ARGS = (
    f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    " --target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
)


def _batch_window() -> tuple[datetime, datetime]:
    """Return the hour this run processes, as a half-open interval [start, end).

    Scheduled runs use their data interval. Runs without a usable interval
    (manual triggers, zero-length intervals) process the full hour before the
    run's logical date, or before "now" when there is no logical date.
    """
    context = get_current_context()
    start, end = context.get("data_interval_start"), context.get("data_interval_end")
    if start is None or end is None or start >= end:
        anchor = context.get("logical_date") or pendulum.now("UTC")
        end = pendulum.instance(anchor).in_tz("UTC").start_of("hour")
        start = end.subtract(hours=1)
    return start, end


def _connection():
    return PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()


@dag(
    dag_id="orders_hourly_pipeline",
    description="Hourly batch pipeline for e-commerce order events with data quality checks",
    # Explicit interval timetable: each run covers the previous full hour [start, end).
    # (Airflow 3 defaults cron schedules to zero-length intervals.)
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # status upserts must be applied hour by hour
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=20),
    },
    params={
        "volume_multiplier": Param(
            1.0,
            type="number",
            minimum=0,
            maximum=3,
            description="Below 1.0 drops a share of source events, to simulate an upstream outage.",
        ),
        "fail_on_anomaly": Param(
            False,
            type="boolean",
            description="Fail the run (and alert) when the volume check flags an anomaly.",
        ),
    },
    tags=["orders", "batch", "data-quality", "dbt"],
    doc_md=__doc__,
)
def orders_hourly_pipeline():
    @task
    def ensure_schema() -> None:
        conn = _connection()
        try:
            loading.ensure_schema(conn)
        finally:
            conn.close()

    @task
    def extract_to_raw() -> int:
        start, end = _batch_window()
        multiplier = float(get_current_context()["params"]["volume_multiplier"])
        events = generate_events(start, end, volume_multiplier=multiplier)
        conn = _connection()
        try:
            landed = loading.land_raw(conn, events, start)
        finally:
            conn.close()
        log.info("Landed %d raw events for %s - %s", landed, start, end)
        return landed

    @task
    def validate_and_load() -> dict:
        start, end = _batch_window()
        conn = _connection()
        try:
            result = validate_batch(loading.read_raw(conn, start), start, end)
            loading.load_validated(conn, result, start, end)
        finally:
            conn.close()
        metrics = {
            "total": result.total,
            "valid": len(result.valid),
            "rejected": len(result.rejected),
            "duplicates": result.duplicates,
        }
        log.info("Validation metrics: %s", metrics)
        return metrics  # small dict only: row data never goes through XCom

    @task
    def upsert_current_orders() -> int:
        start, _ = _batch_window()
        conn = _connection()
        try:
            changed = loading.upsert_current_orders(conn, start)
        finally:
            conn.close()
        log.info("Upserted %d orders", changed)
        return changed

    dbt_env = {
        "WAREHOUSE_HOST": os.environ.get("WAREHOUSE_HOST", "postgres"),
        "WAREHOUSE_PORT": os.environ.get("WAREHOUSE_PORT", "5432"),
        "WAREHOUSE_USER": os.environ.get("WAREHOUSE_USER", "warehouse"),
        "WAREHOUSE_PASSWORD": os.environ.get("WAREHOUSE_PASSWORD", "warehouse"),
        "WAREHOUSE_DB": os.environ.get("WAREHOUSE_DB", "warehouse"),
    }

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_BIN} source freshness {DBT_ARGS}",
        env=dbt_env,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_BIN} build {DBT_ARGS}",
        env=dbt_env,
        append_env=True,
    )

    @task
    def check_volume_anomaly() -> dict:
        start, _ = _batch_window()
        conn = _connection()
        try:
            observed = loading.fetch_valid_count(conn, start)
            history = loading.fetch_same_hour_history(conn, start)
            check = detect_volume_anomaly(observed, history)
            loading.record_anomaly_check(conn, start, check)
        finally:
            conn.close()

        if check.is_anomaly:
            log.warning("Volume anomaly: %s", check.message)
            if get_current_context()["params"]["fail_on_anomaly"]:
                raise AirflowException(f"Volume anomaly detected: {check.message}")
        else:
            log.info(check.message)
        return {"is_anomaly": check.is_anomaly, "z_score": check.z_score, "message": check.message}

    @task
    def apply_retention() -> int:
        start, _ = _batch_window()
        conn = _connection()
        try:
            deleted = loading.apply_retention(conn, start, RAW_RETENTION_DAYS)
        finally:
            conn.close()
        log.info("Deleted %d raw rows older than %d days", deleted, RAW_RETENTION_DAYS)
        return deleted

    chain(
        ensure_schema(),
        extract_to_raw(),
        validate_and_load(),
        upsert_current_orders(),
        dbt_source_freshness,
        dbt_build,
        check_volume_anomaly(),
        apply_retention(),
    )


orders_hourly_pipeline()
