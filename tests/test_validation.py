from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from orders_pipeline.generator import generate_events
from orders_pipeline.validation import DUPLICATE_REASON, validate_batch, validate_event


def test_valid_event_passes(valid_event, window):
    assert validate_event(valid_event, *window) == []


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("customer_id", None, "missing required field: customer_id"),
        ("event_ts", "", "missing required field: event_ts"),
        ("status", "lost_in_transit", "unknown status"),
        ("currency", "XXX", "unsupported currency"),
        ("amount", -10, "amount out of range"),
        ("amount", 0, "amount out of range"),
        ("amount", 250_000, "amount out of range"),
        ("amount", "ten euros", "amount is not numeric"),
        ("country", "Germany", "invalid country code"),
        ("event_ts", "2026-11-05 18:10:00", "event_ts is not a timezone-aware ISO timestamp"),
        ("event_ts", "yesterday", "event_ts is not a timezone-aware ISO timestamp"),
    ],
)
def test_rule_violations_are_reported(valid_event, window, field, value, reason):
    valid_event[field] = value
    assert reason in validate_event(valid_event, *window)


def test_window_is_half_open(valid_event, window):
    start, end = window
    valid_event["event_ts"] = start.isoformat()
    assert validate_event(valid_event, start, end) == []
    valid_event["event_ts"] = end.isoformat()
    assert validate_event(valid_event, start, end) == ["event_ts outside batch window"]
    valid_event["event_ts"] = (start - timedelta(seconds=1)).isoformat()
    assert validate_event(valid_event, start, end) == ["event_ts outside batch window"]


def test_duplicates_are_quarantined_once(valid_event, window):
    result = validate_batch([valid_event, dict(valid_event), dict(valid_event)], *window)
    assert len(result.valid) == 1
    assert result.duplicates == 2
    assert all(r["reasons"] == [DUPLICATE_REASON] for r in result.rejected)


def test_valid_events_are_normalised(valid_event, window):
    (event,) = validate_batch([valid_event], *window).valid
    assert event["amount"] == Decimal("59.90")
    assert event["event_ts"].tzinfo is not None
    assert isinstance(event["customer_id"], int)


def test_nothing_is_lost_on_generated_data(window):
    events = generate_events(*window, dirty_rate=0.05)
    result = validate_batch(events, *window)
    assert result.total == len(events)
    assert result.rejected, "dirty records should be caught"
    assert len({e["event_id"] for e in result.valid}) == len(result.valid)
