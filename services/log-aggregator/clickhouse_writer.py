"""ClickHouse batch writer.

Buffers events in memory and flushes to ClickHouse either when the buffer
reaches BATCH_SIZE rows or FLUSH_INTERVAL_SECONDS elapses — whichever
comes first. This two-condition flush is critical for maintaining low
latency at both high and low throughput.

At 200 events/sec:
  - BATCH_SIZE=5000 fills in ~25 seconds
  - FLUSH_INTERVAL=5 seconds caps worst-case latency
  → In practice, the interval fires first and keeps lag low.
"""

import json
import logging
import os
import time
import threading
from typing import List, Dict, Any

from clickhouse_driver import Client

logger = logging.getLogger(__name__)

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DB = os.getenv("CLICKHOUSE_DB", "cloudmesh")
BATCH_SIZE = int(os.getenv("CLICKHOUSE_BATCH_SIZE", "5000"))
FLUSH_INTERVAL = int(os.getenv("CLICKHOUSE_FLUSH_INTERVAL_SECONDS", "5"))

INSERT_QUERY = f"""
INSERT INTO {CH_DB}.logs
    (timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values)
VALUES
"""


class ClickHouseWriter:
    def __init__(self):
        self._client = Client(
            host=CH_HOST,
            port=CH_PORT,
            compression=True,   # LZ4 compression on the wire
        )
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()

        # Background thread fires interval-based flushes
        self._stop_event = threading.Event()
        self._flush_thread = threading.Thread(target=self._interval_flush_loop, daemon=True)
        self._flush_thread.start()
        logger.info("ClickHouseWriter initialised (batch=%d, interval=%ds)", BATCH_SIZE, FLUSH_INTERVAL)

    def add(self, event: dict) -> None:
        """Add one event to the buffer; flush if batch size reached."""
        with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= BATCH_SIZE:
                self._flush_locked()

    def _interval_flush_loop(self):
        """Background loop: flush every FLUSH_INTERVAL seconds."""
        while not self._stop_event.is_set():
            time.sleep(1)
            if time.time() - self._last_flush >= FLUSH_INTERVAL:
                with self._lock:
                    if self._buffer:
                        self._flush_locked()

    def _flush_locked(self):
        """Flush the buffer to ClickHouse. Must be called with self._lock held."""
        if not self._buffer:
            return

        rows = self._buffer[:]
        self._buffer = []
        self._last_flush = time.time()

        try:
            data = [
                (
                    row["timestamp_ms"],
                    row["event_id"],
                    row["service_name"],
                    row["host"],
                    row["level"],
                    row["message"],
                    row.get("label_keys", []),
                    row.get("label_values", []),
                )
                for row in rows
            ]
            self._client.execute(
                f"INSERT INTO {CH_DB}.logs "
                "(timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values) "
                "VALUES",
                data,
            )
            logger.info("Flushed %d rows to ClickHouse", len(rows))
        except Exception as exc:
            logger.error("ClickHouse insert failed (%d rows): %s", len(rows), exc)
            # Re-add rows to buffer for retry on next flush
            with self._lock:
                self._buffer = rows + self._buffer

    def flush_and_stop(self):
        """Graceful shutdown: flush remaining events and stop the background thread."""
        self._stop_event.set()
        with self._lock:
            self._flush_locked()
