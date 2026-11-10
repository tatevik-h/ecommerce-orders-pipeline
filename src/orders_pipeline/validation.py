"""Row-level data quality rules for order events.

Every event is checked against the expected schema and business rules. Events
that fail are not dropped: they are returned with the list of reasons so the
pipeline can quarantine them for investigation and reprocessing. Reasons are
fixed strings (the offending value stays in the stored payload), so rejection
counts can be grouped and monitored per rule.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

REQUIRED_FIELDS = ("event_id", "order_id", "customer_id", "status", "amount", "currency", "event_ts")
ACCEPTED_STATUSES = frozenset({"placed", "paid", "shipped", "delivered", "cancelled", "refunded"})
ACCEPTED_CURRENCIES = frozenset({"EUR", "USD", "GBP"})
MAX_ORDER_AMOUNT = Decimal("100000")
COUNTRY_CODE = re.compile(r"^[A-Z]{2}$")

DUPLICATE_REASON = "duplicate event_id in batch"


@dataclass
class ValidationResult:
    valid: list[dict] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)
    duplicates: int = 0

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.rejected)


def _parse_ts(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def validate_event(event: dict, window_start: datetime, window_end: datetime) -> list[str]:
    """Return the reasons ``event`` is invalid; an empty list means it passed."""
    reasons = [f"missing required field: {name}" for name in REQUIRED_FIELDS if event.get(name) in (None, "")]
    if reasons:
        return reasons

    if event["status"] not in ACCEPTED_STATUSES:
        reasons.append("unknown status")
    if event["currency"] not in ACCEPTED_CURRENCIES:
        reasons.append("unsupported currency")

    try:
        amount = Decimal(str(event["amount"]))
    except InvalidOperation:
        reasons.append("amount is not numeric")
    else:
        if not Decimal("0") < amount <= MAX_ORDER_AMOUNT:
            reasons.append("amount out of range")

    country = event.get("country")
    if country is not None and not COUNTRY_CODE.match(str(country)):
        reasons.append("invalid country code")

    ts = _parse_ts(event["event_ts"])
    if ts is None or ts.tzinfo is None:
        reasons.append("event_ts is not a timezone-aware ISO timestamp")
    elif not window_start <= ts < window_end:
        reasons.append("event_ts outside batch window")

    return reasons


def _normalise(event: dict) -> dict:
    return {
        "event_id": event["event_id"],
        "order_id": event["order_id"],
        "customer_id": int(event["customer_id"]),
        "status": event["status"],
        "amount": Decimal(str(event["amount"])).quantize(Decimal("0.01")),
        "currency": event["currency"],
        "country": event.get("country"),
        "payment_method": event.get("payment_method"),
        "event_ts": _parse_ts(event["event_ts"]),
    }


def validate_batch(events: list[dict], window_start: datetime, window_end: datetime) -> ValidationResult:
    """Split a batch into normalised valid events and rejected events with reasons."""
    result = ValidationResult()
    seen: set[str] = set()
    for event in events:
        reasons = validate_event(event, window_start, window_end)
        if not reasons and event["event_id"] in seen:
            reasons = [DUPLICATE_REASON]
            result.duplicates += 1
        if reasons:
            result.rejected.append({"event": event, "reasons": reasons})
        else:
            seen.add(event["event_id"])
            result.valid.append(_normalise(event))
    return result
