"""Integration tests against a real Postgres (skipped unless WAREHOUSE_TEST_DSN is set)."""

from __future__ import annotations

from datetime import timedelta

from orders_pipeline import loading
from orders_pipeline.generator import generate_events
from orders_pipeline.validation import validate_batch


def _run_batch(conn, start, end):
    loading.land_raw(conn, generate_events(start, end), start)
    result = validate_batch(loading.read_raw(conn, start), start, end)
    loading.load_validated(conn, result, start, end)
    loading.upsert_current_orders(conn, start)
    return result


def _count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed table names
        return cur.fetchone()[0]


def test_rerunning_a_batch_is_idempotent(pg_conn, window):
    start, end = window
    _run_batch(pg_conn, start, end)
    first = {t: _count(pg_conn, t) for t in ("raw.order_events", "staging.order_events", "quality.rejected_events")}
    _run_batch(pg_conn, start, end)
    second = {t: _count(pg_conn, t) for t in first}
    assert first == second


def test_orders_keep_their_latest_status_even_when_batches_replay_out_of_order(pg_conn, window):
    start, _ = window
    hours = [start + timedelta(hours=h) for h in range(6)]
    for h in hours:
        _run_batch(pg_conn, h, h + timedelta(hours=1))
    with pg_conn.cursor() as cur:
        cur.execute("SELECT order_id, current_status FROM staging.orders ORDER BY order_id")
        expected = cur.fetchall()

    # Backfill an older hour again: it must not roll any order back to an earlier status.
    _run_batch(pg_conn, hours[0], hours[0] + timedelta(hours=1))
    with pg_conn.cursor() as cur:
        cur.execute("SELECT order_id, current_status FROM staging.orders ORDER BY order_id")
        assert cur.fetchall() == expected


def test_batch_metrics_and_retention(pg_conn, window):
    start, end = window
    result = _run_batch(pg_conn, start, end)
    assert loading.fetch_valid_count(pg_conn, start) == len(result.valid)

    deleted = loading.apply_retention(pg_conn, start + timedelta(days=31), raw_retention_days=30)
    assert deleted == result.total
    assert _count(pg_conn, "raw.order_events") == 0
