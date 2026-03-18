"""Integration tests for ClickHouse write path.

These tests require a running ClickHouse instance.
Run with: pytest tests/test_clickhouse_write.py -v -k "integration"

In CI, ClickHouse is started as a GitHub Actions service container.
"""

import os
import time
import uuid

import pytest

# Mark all tests in this file as integration
pytestmark = pytest.mark.integration

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))


@pytest.fixture(scope="module")
def ch_client():
    pytest.importorskip("clickhouse_driver", reason="clickhouse-driver not installed")
    from clickhouse_driver import Client
    client = Client(host=CH_HOST, port=CH_PORT)
    # Ensure schema exists
    client.execute("CREATE DATABASE IF NOT EXISTS cloudmesh")
    client.execute("""
        CREATE TABLE IF NOT EXISTS cloudmesh.logs_test
        (
            event_date   Date        MATERIALIZED toDate(toDateTime(timestamp_ms / 1000)),
            timestamp_ms Int64,
            event_id     String,
            service_name LowCardinality(String),
            host         LowCardinality(String),
            level        LowCardinality(String),
            message      String,
            is_error     UInt8 MATERIALIZED (level IN ('ERROR','FATAL') ? 1 : 0),
            label_keys   Array(String),
            label_values Array(String)
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (service_name, level, timestamp_ms)
    """)
    yield client
    client.execute("DROP TABLE IF EXISTS cloudmesh.logs_test")


def test_insert_single_row(ch_client):
    now_ms = int(time.time() * 1000)
    ch_client.execute(
        "INSERT INTO cloudmesh.logs_test "
        "(timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values) "
        "VALUES",
        [(now_ms, str(uuid.uuid4()), "payment-service", "pod-01", "INFO", "test message", [], [])],
    )
    rows = ch_client.execute(
        "SELECT count() FROM cloudmesh.logs_test WHERE service_name = 'payment-service'"
    )
    assert rows[0][0] >= 1


def test_low_cardinality_columns_accept_values(ch_client):
    now_ms = int(time.time() * 1000)
    for level in ("DEBUG", "INFO", "WARN", "ERROR", "FATAL"):
        ch_client.execute(
            "INSERT INTO cloudmesh.logs_test "
            "(timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values) "
            "VALUES",
            [(now_ms, str(uuid.uuid4()), "auth-service", "pod-02", level, f"{level} message", [], [])],
        )
    rows = ch_client.execute(
        "SELECT count() FROM cloudmesh.logs_test WHERE service_name = 'auth-service'"
    )
    assert rows[0][0] >= 5


def test_batch_insert_performance(ch_client):
    """Insert 1000 rows and verify all are written — basic throughput sanity check."""
    now_ms = int(time.time() * 1000)
    batch_id = str(uuid.uuid4())[:8]
    rows = [
        (now_ms + i, str(uuid.uuid4()), "inventory-service", "pod-03", "INFO", f"msg-{batch_id}-{i}", [], [])
        for i in range(1000)
    ]
    start = time.perf_counter()
    ch_client.execute(
        "INSERT INTO cloudmesh.logs_test "
        "(timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values) "
        "VALUES",
        rows,
    )
    elapsed = time.perf_counter() - start
    assert elapsed < 5.0, f"Batch insert of 1000 rows took too long: {elapsed:.2f}s"

    count = ch_client.execute(
        f"SELECT count() FROM cloudmesh.logs_test WHERE message LIKE 'msg-{batch_id}-%'"
    )[0][0]
    assert count == 1000


def test_label_arrays_round_trip(ch_client):
    now_ms = int(time.time() * 1000)
    event_id = str(uuid.uuid4())
    ch_client.execute(
        "INSERT INTO cloudmesh.logs_test "
        "(timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values) "
        "VALUES",
        [(now_ms, event_id, "api-gateway", "pod-04", "INFO", "labelled event",
          ["trace_id", "env"], ["abc-123", "production"])],
    )
    rows = ch_client.execute(
        "SELECT label_keys, label_values FROM cloudmesh.logs_test WHERE event_id = %(eid)s",
        {"eid": event_id},
    )
    assert len(rows) == 1
    keys, values = rows[0]
    assert "trace_id" in keys
    assert "abc-123" in values
