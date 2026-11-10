"""Statistical volume check for each hourly batch.

Row-level rules can't catch everything: a batch can be perfectly valid and
still be half the size it should be because an upstream system dropped data.
This check compares the number of valid events with the same hour of the day
over the previous days and flags large deviations using a z-score.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class AnomalyResult:
    is_anomaly: bool
    observed: int
    baseline_mean: float | None
    baseline_std: float | None
    z_score: float | None
    message: str


def detect_volume_anomaly(
    observed: int,
    history: Sequence[int],
    z_threshold: float = 3.0,
    min_history: int = 3,
) -> AnomalyResult:
    """Flag ``observed`` if it is more than ``z_threshold`` deviations from ``history``.

    The standard deviation has a floor of 5% of the mean, so a very stable
    history doesn't turn small, harmless changes into alerts.
    """
    if len(history) < min_history:
        return AnomalyResult(
            False,
            observed,
            None,
            None,
            None,
            f"not enough history ({len(history)} of {min_history} points), check skipped",
        )

    mean = statistics.fmean(history)
    std = max(statistics.pstdev(history), mean * 0.05, 1.0)
    z = (observed - mean) / std
    is_anomaly = abs(z) > z_threshold
    direction = "above" if z > 0 else "below"
    message = (
        f"volume {observed} is {abs(z):.1f} std {direction} baseline {mean:.0f}"
        if is_anomaly
        else f"volume {observed} within normal range (baseline {mean:.0f}, z={z:.2f})"
    )
    return AnomalyResult(is_anomaly, observed, round(mean, 2), round(std, 2), round(z, 3), message)
