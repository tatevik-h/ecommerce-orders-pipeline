from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from orders_pipeline.generator import generate_events, orders_per_hour


def test_generation_is_deterministic(window):
    assert generate_events(*window) == generate_events(*window)


def test_clean_events_fall_inside_the_window(window):
    start, end = window
    events = generate_events(start, end, dirty_rate=0)
    assert events, "expected events for an evening hour"
    for event in events:
        assert start <= datetime.fromisoformat(event["event_ts"]) < end


def test_event_ids_are_unique_without_dirty_records(window):
    events = generate_events(*window, dirty_rate=0)
    ids = [e["event_id"] for e in events]
    assert len(ids) == len(set(ids))


def test_dirty_records_are_injected(window):
    clean = generate_events(*window, dirty_rate=0)
    dirty = generate_events(*window, dirty_rate=0.2)
    assert dirty != clean
    assert len(dirty) >= len(clean)  # duplicates add rows, other defects corrupt in place


def test_volume_multiplier_simulates_an_outage(window):
    full = generate_events(*window, dirty_rate=0)
    half = generate_events(*window, volume_multiplier=0.5, dirty_rate=0)
    assert 0.35 * len(full) < len(half) < 0.65 * len(full)
    assert generate_events(*window, volume_multiplier=0) == []


def test_daily_seasonality(window):
    start, _ = window
    night = start.replace(hour=3)
    evening = start.replace(hour=20)
    assert orders_per_hour(evening) > 3 * orders_per_hour(night)


def test_lifecycle_spans_multiple_hours(window):
    start, _ = window
    statuses = {e["status"] for e in generate_events(start, start + timedelta(hours=3), dirty_rate=0)}
    assert {"placed", "paid", "shipped", "delivered"} <= statuses


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError):
        generate_events(datetime(2026, 11, 5, 18), datetime(2026, 11, 5, 19))
