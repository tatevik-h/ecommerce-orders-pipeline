from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

WINDOW_START = datetime(2026, 11, 5, 18, tzinfo=timezone.utc)
WINDOW_END = WINDOW_START + timedelta(hours=1)


@pytest.fixture
def window() -> tuple[datetime, datetime]:
    return WINDOW_START, WINDOW_END


@pytest.fixture
def valid_event() -> dict:
    return {
        "event_id": "ORD-2026110518-00001:paid",
        "order_id": "ORD-2026110518-00001",
        "customer_id": 42,
        "status": "paid",
        "amount": 59.90,
        "currency": "EUR",
        "country": "DE",
        "payment_method": "card",
        "event_ts": (WINDOW_START + timedelta(minutes=10)).isoformat(),
    }


@pytest.fixture
def pg_conn():
    """Connection to a disposable Postgres database; integration tests skip without one.

    Set WAREHOUSE_TEST_DSN, e.g. postgresql://warehouse:warehouse@localhost:5432/warehouse_test
    """
    dsn = os.environ.get("WAREHOUSE_TEST_DSN")
    if not dsn:
        pytest.skip("WAREHOUSE_TEST_DSN not set")
    psycopg2 = pytest.importorskip("psycopg2")

    from orders_pipeline import loading

    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw, staging, quality CASCADE")
    conn.commit()
    loading.ensure_schema(conn)
    yield conn
    conn.close()
