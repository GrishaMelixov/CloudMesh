"""Unit tests for gRPC server logic (no network required).

Tests the LogCollectorServicer directly by calling its methods with
in-process objects — no actual gRPC channel needed.
"""

import sys
import os
import time
import uuid

import pytest

# We mock the kafka_producer so the test doesn't need a running Kafka
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/log-collector"))


class MockKafkaProducer:
    def __init__(self):
        self.sent = []

    def send(self, event_dict):
        self.sent.append(event_dict)

    def flush(self):
        pass


class MockContext:
    """Minimal stand-in for grpc.ServicerContext."""
    pass


def make_log_event(service="payment-service", level="INFO", message="test"):
    """Helper: create a minimal LogEvent protobuf-like object."""
    # We import after sys.path is set up
    import logs_pb2
    event = logs_pb2.LogEvent(
        event_id=str(uuid.uuid4()),
        service_name=service,
        host="test-pod-01",
        level=level,
        message=message,
        timestamp_ms=int(time.time() * 1000),
    )
    event.labels["env"] = "test"
    return event


@pytest.fixture
def servicer_and_kafka():
    """Return (servicer, mock_kafka) with a fresh MockKafkaProducer."""
    from grpc_server import LogCollectorServicer
    kafka = MockKafkaProducer()
    servicer = LogCollectorServicer(kafka)
    return servicer, kafka


# ── Skip if protobuf stubs are not generated yet ──────────────────────────────
try:
    import logs_pb2
    import logs_pb2_grpc
    HAS_STUBS = True
except ImportError:
    HAS_STUBS = False

pytestmark = pytest.mark.skipif(not HAS_STUBS, reason="Proto stubs not generated")


def test_collect_batch_accepts_valid_events(servicer_and_kafka):
    servicer, kafka = servicer_and_kafka
    import logs_pb2

    batch = logs_pb2.LogBatch(
        events=[make_log_event("payment-service", "INFO") for _ in range(5)],
        producer_id="test-producer",
    )
    response = servicer.CollectBatch(batch, MockContext())
    assert response.accepted == 5
    assert response.rejected == 0
    assert len(kafka.sent) == 5


def test_collect_batch_rejects_missing_service_name(servicer_and_kafka):
    servicer, kafka = servicer_and_kafka
    import logs_pb2

    bad_event = logs_pb2.LogEvent(
        event_id=str(uuid.uuid4()),
        service_name="",   # missing!
        level="INFO",
        message="test",
        timestamp_ms=int(time.time() * 1000),
    )
    batch = logs_pb2.LogBatch(events=[bad_event], producer_id="test")
    response = servicer.CollectBatch(batch, MockContext())
    assert response.rejected == 1
    assert response.accepted == 0
    assert len(kafka.sent) == 0


def test_collect_batch_rejects_invalid_level(servicer_and_kafka):
    servicer, kafka = servicer_and_kafka
    import logs_pb2

    bad_event = logs_pb2.LogEvent(
        service_name="auth-service",
        level="NOTLEVEL",   # invalid level
        message="hello",
        timestamp_ms=int(time.time() * 1000),
    )
    batch = logs_pb2.LogBatch(events=[bad_event], producer_id="test")
    response = servicer.CollectBatch(batch, MockContext())
    assert response.rejected == 1


def test_collect_batch_mixed_valid_invalid(servicer_and_kafka):
    servicer, kafka = servicer_and_kafka
    import logs_pb2

    events = [
        make_log_event("payment-service", "INFO"),
        logs_pb2.LogEvent(service_name="", level="INFO", message="bad"),
        make_log_event("auth-service", "ERROR"),
    ]
    batch = logs_pb2.LogBatch(events=events, producer_id="test")
    response = servicer.CollectBatch(batch, MockContext())
    assert response.accepted == 2
    assert response.rejected == 1


def test_event_dict_contains_required_fields(servicer_and_kafka):
    servicer, kafka = servicer_and_kafka
    import logs_pb2

    batch = logs_pb2.LogBatch(
        events=[make_log_event("inventory-service", "WARN", "low stock")],
        producer_id="test",
    )
    servicer.CollectBatch(batch, MockContext())
    assert len(kafka.sent) == 1
    sent = kafka.sent[0]
    for field in ("event_id", "service_name", "level", "message", "timestamp_ms"):
        assert field in sent, f"Missing field: {field}"
    assert sent["service_name"] == "inventory-service"
    assert sent["level"] == "WARN"


def test_stream_logs(servicer_and_kafka):
    servicer, kafka = servicer_and_kafka

    def event_stream():
        for i in range(10):
            yield make_log_event("api-gateway", "INFO", f"request {i}")

    response = servicer.StreamLogs(event_stream(), MockContext())
    assert response.accepted == 10
    assert len(kafka.sent) == 10
