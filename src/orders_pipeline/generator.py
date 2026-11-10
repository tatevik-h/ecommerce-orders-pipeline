"""Synthetic source system that emits e-commerce order events for an hourly window.

Each order's whole lifecycle (placed -> paid -> shipped -> delivered, with
cancellations and refunds) is derived from a seed built from its ``order_id``.
Generating the same window twice therefore always returns the same events,
which behaves like re-reading a partition from a real source system and keeps
reruns and backfills idempotent.

A small share of events is deliberately corrupted (missing fields, invalid
values, duplicates) so the validation layer has something real to catch.
"""

from __future__ import annotations

import copy
import hashlib
import random
from datetime import datetime, timedelta

ORDER_STATUSES = ("placed", "paid", "shipped", "delivered", "cancelled", "refunded")
CURRENCY_WEIGHTS = {"EUR": 0.6, "USD": 0.25, "GBP": 0.15}
COUNTRIES = ("DE", "FR", "PL", "ES", "IT", "NL", "AM", "GB", "US")
PAYMENT_METHODS = ("card", "paypal", "bank_transfer", "apple_pay")

BASE_ORDERS_PER_HOUR = 200
# Demand multiplier per UTC hour: quiet at night, peak in the evening.
# fmt: off
HOURLY_PROFILE = (
    0.35, 0.25, 0.20, 0.18, 0.20, 0.30, 0.45, 0.60,
    0.75, 0.85, 0.90, 0.95, 1.00, 0.95, 0.90, 0.90,
    0.95, 1.05, 1.20, 1.35, 1.40, 1.25, 0.90, 0.55,
)
# fmt: on
# Longest possible lifecycle is ~95 hours, so older orders can't emit new events.
LOOKBACK_HOURS = 96


def _seed(*parts: object) -> int:
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(digest[:8], "big")


def orders_per_hour(hour_start: datetime) -> int:
    """Number of orders placed in the hour starting at ``hour_start``."""
    rng = random.Random(_seed("volume", hour_start.isoformat()))
    expected = BASE_ORDERS_PER_HOUR * HOURLY_PROFILE[hour_start.hour]
    return max(0, round(expected * rng.uniform(0.9, 1.1)))


def _lifecycle(placed_at: datetime, rng: random.Random) -> list[tuple[str, datetime]]:
    """Return the ordered (status, timestamp) transitions of one order."""
    steps = [("placed", placed_at)]
    if rng.random() < 0.08:
        steps.append(("cancelled", placed_at + timedelta(minutes=rng.uniform(5, 120))))
        return steps

    paid_at = placed_at + timedelta(minutes=rng.uniform(1, 30))
    steps.append(("paid", paid_at))
    if rng.random() < 0.02:
        steps.append(("cancelled", paid_at + timedelta(hours=rng.uniform(1, 6))))
        return steps

    shipped_at = paid_at + timedelta(hours=rng.uniform(2, 20))
    delivered_at = shipped_at + timedelta(hours=rng.uniform(12, 48))
    steps += [("shipped", shipped_at), ("delivered", delivered_at)]
    if rng.random() < 0.04:
        steps.append(("refunded", delivered_at + timedelta(hours=rng.uniform(1, 24))))
    return steps


def _orders_placed_in(hour_start: datetime) -> list[tuple[dict, list[tuple[str, datetime]]]]:
    orders = []
    for i in range(orders_per_hour(hour_start)):
        order_id = f"ORD-{hour_start:%Y%m%d%H}-{i:05d}"
        rng = random.Random(_seed(order_id))
        placed_at = hour_start + timedelta(seconds=rng.uniform(0, 3600))
        attributes = {
            "order_id": order_id,
            "customer_id": rng.randint(1, 50_000),
            "amount": round(rng.lognormvariate(3.8, 0.7), 2),
            "currency": rng.choices(list(CURRENCY_WEIGHTS), weights=list(CURRENCY_WEIGHTS.values()))[0],
            "country": rng.choice(COUNTRIES),
            "payment_method": rng.choice(PAYMENT_METHODS),
        }
        orders.append((attributes, _lifecycle(placed_at, rng)))
    return orders


def _inject_dirty_records(events: list[dict], dirty_rate: float, rng: random.Random) -> list[dict]:
    """Corrupt a small, deterministic share of events the way real sources do."""
    dirty = []
    for event in events:
        if rng.random() >= dirty_rate:
            dirty.append(event)
            continue
        broken = copy.deepcopy(event)
        defect = rng.choice(("missing_customer", "negative_amount", "unknown_currency", "bad_status", "duplicate"))
        if defect == "missing_customer":
            broken["customer_id"] = None
        elif defect == "negative_amount":
            broken["amount"] = -broken["amount"]
        elif defect == "unknown_currency":
            broken["currency"] = "XXX"
        elif defect == "bad_status":
            broken["status"] = "lost_in_transit"
        else:  # at-least-once delivery: the same event arrives twice
            dirty.append(event)
        dirty.append(broken)
    return dirty


def generate_events(
    window_start: datetime,
    window_end: datetime,
    volume_multiplier: float = 1.0,
    dirty_rate: float = 0.02,
) -> list[dict]:
    """Return all order events whose timestamp falls in ``[window_start, window_end)``.

    ``volume_multiplier`` below 1.0 drops a share of events, which simulates a
    partial outage upstream (useful for demonstrating the anomaly check).
    """
    if window_start.tzinfo is None or window_end.tzinfo is None:
        raise ValueError("window boundaries must be timezone-aware")

    events: list[dict] = []
    hour = (window_start - timedelta(hours=LOOKBACK_HOURS)).replace(minute=0, second=0, microsecond=0)
    while hour < window_end:
        for attributes, steps in _orders_placed_in(hour):
            for status, ts in steps:
                if not window_start <= ts < window_end:
                    continue
                event_id = f"{attributes['order_id']}:{status}"
                keep = random.Random(_seed("keep", event_id)).random() < volume_multiplier
                if keep:
                    events.append({"event_id": event_id, **attributes, "status": status, "event_ts": ts.isoformat()})
        hour += timedelta(hours=1)

    events.sort(key=lambda e: e["event_ts"])
    rng = random.Random(_seed("dirty", window_start.isoformat()))
    return _inject_dirty_records(events, dirty_rate, rng)
