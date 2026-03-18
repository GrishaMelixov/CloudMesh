"""Unit tests for the sliding-window error rate monitor.

These tests run without any external dependencies (no Kafka, no ClickHouse).
"""

import time
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/log-aggregator"))

from error_rate_monitor import ErrorRateMonitor


def test_no_alert_below_threshold():
    alerts = []
    monitor = ErrorRateMonitor(
        window_seconds=60,
        threshold_pct=10.0,
        on_alert=lambda svc, rate, count: alerts.append((svc, rate, count)),
    )
    for _ in range(100):
        monitor.record("payment-service", "INFO")
    # 0% error rate — no alert expected
    assert len(alerts) == 0


def test_alert_fires_above_threshold():
    alerts = []
    monitor = ErrorRateMonitor(
        window_seconds=60,
        threshold_pct=10.0,
        on_alert=lambda svc, rate, count: alerts.append((svc, rate, count)),
    )
    # 9 INFO + 1 ERROR = 10% error rate → exactly at threshold → should fire
    for _ in range(9):
        monitor.record("payment-service", "INFO")
    monitor.record("payment-service", "ERROR")
    assert len(alerts) == 1
    assert alerts[0][0] == "payment-service"
    assert alerts[0][1] >= 10.0


def test_alert_fires_for_fatal_level():
    alerts = []
    monitor = ErrorRateMonitor(
        window_seconds=60,
        threshold_pct=5.0,
        on_alert=lambda svc, rate, count: alerts.append((svc, rate, count)),
    )
    for _ in range(19):
        monitor.record("auth-service", "INFO")
    monitor.record("auth-service", "FATAL")  # FATAL counts as error
    assert len(alerts) >= 1


def test_stats_returns_correct_counts():
    monitor = ErrorRateMonitor(window_seconds=60, threshold_pct=50.0, on_alert=None)
    for _ in range(8):
        monitor.record("inventory-service", "INFO")
    for _ in range(2):
        monitor.record("inventory-service", "ERROR")

    stats = monitor.get_stats("inventory-service")
    assert stats["total"] == 10
    assert stats["errors"] == 2
    assert stats["error_rate_pct"] == 20.0


def test_window_evicts_old_entries():
    """Events older than window_seconds should not contribute to the error rate."""
    alerts = []
    monitor = ErrorRateMonitor(
        window_seconds=1,  # 1-second window for fast testing
        threshold_pct=10.0,
        on_alert=lambda svc, rate, count: alerts.append((svc, rate, count)),
    )
    # Send 20 errors — should trigger alerts
    for _ in range(20):
        monitor.record("api-gateway", "ERROR")

    alert_count_before = len(alerts)
    assert alert_count_before > 0

    # Wait for the window to expire
    time.sleep(1.2)

    # Reset alert list and send 1 INFO — window is now empty, no alert expected
    alerts.clear()
    monitor.record("api-gateway", "INFO")

    stats = monitor.get_stats("api-gateway")
    assert stats["total"] == 1, "Stale entries should be evicted after window expires"
    assert stats["errors"] == 0


def test_multiple_services_are_independent():
    alerts = []
    monitor = ErrorRateMonitor(
        window_seconds=60,
        threshold_pct=10.0,
        on_alert=lambda svc, rate, count: alerts.append(svc),
    )
    # Only payment-service has errors
    for _ in range(9):
        monitor.record("payment-service", "INFO")
    monitor.record("payment-service", "ERROR")

    for _ in range(100):
        monitor.record("auth-service", "INFO")

    alerted_services = set(alerts)
    assert "payment-service" in alerted_services
    assert "auth-service" not in alerted_services


def test_no_alert_when_callback_is_none():
    """Monitor should work without an alert callback (no exception)."""
    monitor = ErrorRateMonitor(window_seconds=60, threshold_pct=5.0, on_alert=None)
    for _ in range(100):
        monitor.record("service-x", "ERROR")  # Should not raise
