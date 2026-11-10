from __future__ import annotations

from orders_pipeline.anomaly import detect_volume_anomaly

HISTORY = [610, 640, 655, 600, 625, 648, 632]


def test_skipped_without_enough_history():
    result = detect_volume_anomaly(10, [600, 610])
    assert not result.is_anomaly
    assert result.z_score is None


def test_normal_volume_is_not_flagged():
    result = detect_volume_anomaly(620, HISTORY)
    assert not result.is_anomaly
    assert abs(result.z_score) < 3


def test_drop_is_flagged():
    result = detect_volume_anomaly(300, HISTORY)
    assert result.is_anomaly
    assert result.z_score < -3
    assert "below" in result.message


def test_spike_is_flagged():
    result = detect_volume_anomaly(1200, HISTORY)
    assert result.is_anomaly
    assert result.z_score > 3


def test_flat_history_does_not_divide_by_zero():
    flat = [500] * 7
    assert not detect_volume_anomaly(510, flat).is_anomaly  # within the 5% std floor
    assert detect_volume_anomaly(300, flat).is_anomaly
