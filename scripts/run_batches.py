"""Run the pipeline's Python steps for a range of hours without Airflow.

Useful for seeding a local warehouse, for CI, and for quick experiments:

    python scripts/run_batches.py --start 2026-11-01T00:00:00+00:00 --hours 48

Connection settings come from the WAREHOUSE_* environment variables.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

import psycopg2

from orders_pipeline import loading
from orders_pipeline.anomaly import detect_volume_anomaly
from orders_pipeline.generator import generate_events
from orders_pipeline.validation import validate_batch


def connect():
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        user=os.environ.get("WAREHOUSE_USER", "warehouse"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "warehouse"),
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
    )


def run_batch(conn, start: datetime, volume_multiplier: float = 1.0) -> dict:
    end = start + timedelta(hours=1)
    loading.land_raw(conn, generate_events(start, end, volume_multiplier=volume_multiplier), start)
    result = validate_batch(loading.read_raw(conn, start), start, end)
    loading.load_validated(conn, result, start, end)
    loading.upsert_current_orders(conn, start)
    check = detect_volume_anomaly(len(result.valid), loading.fetch_same_hour_history(conn, start))
    loading.record_anomaly_check(conn, start, check)
    return {"valid": len(result.valid), "rejected": len(result.rejected), "check": check.message}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True, type=datetime.fromisoformat, help="first hour, ISO with timezone")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--volume-multiplier", type=float, default=1.0)
    args = parser.parse_args()
    if args.start.tzinfo is None:
        parser.error("--start must include a timezone, e.g. 2026-11-01T00:00:00+00:00")

    conn = connect()
    try:
        loading.ensure_schema(conn)
        for i in range(args.hours):
            start = args.start + timedelta(hours=i)
            summary = run_batch(conn, start, args.volume_multiplier)
            print(
                f"{start.isoformat()}  valid={summary['valid']:>5}"
                f"  rejected={summary['rejected']:>3}  {summary['check']}"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
