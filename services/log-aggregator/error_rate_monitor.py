"""Sliding-window error rate monitor.

Maintains a deque of (timestamp, is_error) tuples per service.
Evicts entries older than WINDOW_SECONDS on each update.
Fires a callback when the error rate exceeds THRESHOLD_PCT.

This is an in-memory, per-aggregator-instance monitor.
For multi-instance deployments, use the ClickHouse materialised view
(error_rate_per_minute) for cluster-wide accuracy.
"""

import logging
import time
from collections import defaultdict, deque
from typing import Callable, Deque, Tuple

logger = logging.getLogger(__name__)

# Type alias: deque of (timestamp_float, is_error_bool)
_Window = Deque[Tuple[float, bool]]


class ErrorRateMonitor:
    def __init__(
        self,
        window_seconds: int = 60,
        threshold_pct: float = 10.0,
        on_alert: Callable[[str, float, int], None] = None,
    ):
        self._window = window_seconds
        self._threshold = threshold_pct
        self._on_alert = on_alert
        # service_name → deque of (ts, is_error)
        self._windows: dict[str, _Window] = defaultdict(deque)

    def record(self, service_name: str, level: str) -> None:
        """Record one event. Triggers alert callback if threshold exceeded."""
        now = time.monotonic()
        is_error = level.upper() in ("ERROR", "FATAL")
        window = self._windows[service_name]

        # Append new entry
        window.append((now, is_error))

        # Evict entries outside the sliding window (left side of deque)
        cutoff = now - self._window
        while window and window[0][0] < cutoff:
            window.popleft()

        # Compute current error rate
        total = len(window)
        if total == 0:
            return
        errors = sum(1 for _, err in window if err)
        error_rate = errors / total * 100

        if error_rate >= self._threshold and self._on_alert:
            logger.warning(
                "Error rate alert: service=%s rate=%.1f%% (threshold=%.1f%%) window=%ds events=%d",
                service_name, error_rate, self._threshold, self._window, total,
            )
            self._on_alert(service_name, error_rate, total)

    def get_stats(self, service_name: str) -> dict:
        """Return current stats for a service (for debugging/testing)."""
        window = self._windows.get(service_name, deque())
        now = time.monotonic()
        cutoff = now - self._window
        active = [(ts, err) for ts, err in window if ts >= cutoff]
        total = len(active)
        errors = sum(1 for _, err in active if err)
        return {
            "service_name": service_name,
            "total": total,
            "errors": errors,
            "error_rate_pct": round(errors / total * 100, 2) if total else 0.0,
        }
