"""Warehouse I/O for the pipeline.

Every write is scoped to one batch (``batch_start``) and is idempotent:
re-running a batch replaces its raw and rejected rows and upserts its valid
events, so retries and backfills never create duplicates.

Functions take a plain DB-API connection (psycopg2 or psycopg 3) so they can
be called from Airflow tasks, scripts and integration tests alike.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from orders_pipeline.anomaly import AnomalyResult
from orders_pipeline.validation import ValidationResult

SCHEMA_SQL = Path(__file__).with_name("sql") / "warehouse_schema.sql"


@contextmanager
def transaction(conn) -> Iterator[object]:
    """Yield a cursor and commit on success, roll back on any error."""
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def ensure_schema(conn) -> None:
    with transaction(conn) as cur:
        cur.execute(SCHEMA_SQL.read_text())


def land_raw(conn, events: Sequence[dict], batch_start: datetime) -> int:
    """Replace the raw landing rows of a batch with ``events`` exactly as received."""
    with transaction(conn) as cur:
        cur.execute("DELETE FROM raw.order_events WHERE batch_start = %s", (batch_start,))
        if events:
            cur.executemany(
                "INSERT INTO raw.order_events (batch_start, payload) VALUES (%s, %s::jsonb)",
                [(batch_start, json.dumps(e, default=str)) for e in events],
            )
    return len(events)


def read_raw(conn, batch_start: datetime) -> list[dict]:
    with transaction(conn) as cur:
        cur.execute(
            "SELECT payload FROM raw.order_events WHERE batch_start = %s ORDER BY raw_id",
            (batch_start,),
        )
        rows = cur.fetchall()
    return [row[0] if isinstance(row[0], dict) else json.loads(row[0]) for row in rows]


def load_validated(conn, result: ValidationResult, batch_start: datetime, batch_end: datetime) -> None:
    """Quarantine rejected events, upsert valid ones and record batch metrics in one transaction."""
    with transaction(conn) as cur:
        cur.execute("DELETE FROM quality.rejected_events WHERE batch_start = %s", (batch_start,))
        if result.rejected:
            cur.executemany(
                "INSERT INTO quality.rejected_events (batch_start, event_id, payload, reasons)"
                " VALUES (%s, %s, %s::jsonb, %s)",
                [
                    (batch_start, r["event"].get("event_id"), json.dumps(r["event"], default=str), r["reasons"])
                    for r in result.rejected
                ],
            )

        if result.valid:
            cur.executemany(
                """
                INSERT INTO staging.order_events
                    (event_id, order_id, customer_id, status, amount, currency,
                     country, payment_method, event_ts, batch_start)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    customer_id    = EXCLUDED.customer_id,
                    status         = EXCLUDED.status,
                    amount         = EXCLUDED.amount,
                    currency       = EXCLUDED.currency,
                    country        = EXCLUDED.country,
                    payment_method = EXCLUDED.payment_method,
                    event_ts       = EXCLUDED.event_ts,
                    batch_start    = EXCLUDED.batch_start,
                    loaded_at      = now()
                """,
                [
                    (
                        e["event_id"],
                        e["order_id"],
                        e["customer_id"],
                        e["status"],
                        e["amount"],
                        e["currency"],
                        e["country"],
                        e["payment_method"],
                        e["event_ts"],
                        batch_start,
                    )
                    for e in result.valid
                ],
            )

        cur.execute(
            """
            INSERT INTO quality.batch_metrics
                (batch_start, batch_end, total_events, valid_events, rejected_events, duplicate_events)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_start) DO UPDATE SET
                batch_end        = EXCLUDED.batch_end,
                total_events     = EXCLUDED.total_events,
                valid_events     = EXCLUDED.valid_events,
                rejected_events  = EXCLUDED.rejected_events,
                duplicate_events = EXCLUDED.duplicate_events,
                computed_at      = now()
            """,
            (batch_start, batch_end, result.total, len(result.valid), len(result.rejected), result.duplicates),
        )


def upsert_current_orders(conn, batch_start: datetime) -> int:
    """Apply the batch's status changes to ``staging.orders`` (one row per order, latest status).

    The ``WHERE`` guard makes the upsert order-independent: an older event
    replayed by a backfill can never overwrite a newer status.
    """
    with transaction(conn) as cur:
        cur.execute(
            """
            INSERT INTO staging.orders
                (order_id, customer_id, current_status, amount, currency, country,
                 payment_method, last_event_ts)
            SELECT DISTINCT ON (order_id)
                order_id, customer_id, status, amount, currency, country, payment_method, event_ts
            FROM staging.order_events
            WHERE batch_start = %s
            ORDER BY order_id, event_ts DESC
            ON CONFLICT (order_id) DO UPDATE SET
                current_status = EXCLUDED.current_status,
                last_event_ts  = EXCLUDED.last_event_ts,
                updated_at     = now()
            WHERE staging.orders.last_event_ts <= EXCLUDED.last_event_ts
            """,
            (batch_start,),
        )
        return cur.rowcount


def fetch_same_hour_history(conn, batch_start: datetime, days: int = 7) -> list[int]:
    """Valid event counts for the same hour of the day over the previous ``days`` days."""
    earlier = [batch_start - timedelta(days=d) for d in range(1, days + 1)]
    with transaction(conn) as cur:
        cur.execute(
            "SELECT valid_events FROM quality.batch_metrics WHERE batch_start = ANY(%s)",
            (earlier,),
        )
        return [row[0] for row in cur.fetchall()]


def fetch_valid_count(conn, batch_start: datetime) -> int:
    with transaction(conn) as cur:
        cur.execute("SELECT valid_events FROM quality.batch_metrics WHERE batch_start = %s", (batch_start,))
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"no batch metrics recorded for {batch_start.isoformat()}")
    return row[0]


def record_anomaly_check(conn, batch_start: datetime, check: AnomalyResult) -> None:
    with transaction(conn) as cur:
        cur.execute(
            """
            INSERT INTO quality.volume_checks
                (batch_start, observed, baseline_mean, baseline_std, z_score, is_anomaly, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_start) DO UPDATE SET
                observed      = EXCLUDED.observed,
                baseline_mean = EXCLUDED.baseline_mean,
                baseline_std  = EXCLUDED.baseline_std,
                z_score       = EXCLUDED.z_score,
                is_anomaly    = EXCLUDED.is_anomaly,
                message       = EXCLUDED.message,
                checked_at    = now()
            """,
            (
                batch_start,
                check.observed,
                check.baseline_mean,
                check.baseline_std,
                check.z_score,
                check.is_anomaly,
                check.message,
            ),
        )


def apply_retention(conn, batch_start: datetime, raw_retention_days: int = 30) -> int:
    """Delete raw landing data older than the retention period. Returns rows deleted."""
    cutoff = batch_start - timedelta(days=raw_retention_days)
    with transaction(conn) as cur:
        cur.execute("DELETE FROM raw.order_events WHERE batch_start < %s", (cutoff,))
        return cur.rowcount
