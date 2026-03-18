"""gRPC server implementation for log-collector.

Implements the LogCollector service defined in proto/logs.proto.
Receives log batches from producers and publishes them to Kafka.
"""

import logging
import time
import uuid
from concurrent import futures

import grpc

# These modules are generated at build time from logs.proto
import logs_pb2
import logs_pb2_grpc

from kafka_producer import KafkaLogProducer

logger = logging.getLogger(__name__)

VALID_LEVELS = {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"}


def _proto_event_to_dict(event: logs_pb2.LogEvent) -> dict:
    """Convert a protobuf LogEvent to a plain dict for Kafka serialisation."""
    return {
        "event_id": event.event_id or str(uuid.uuid4()),
        "service_name": event.service_name,
        "host": event.host,
        "level": event.level.upper(),
        "message": event.message,
        "timestamp_ms": event.timestamp_ms or int(time.time() * 1000),
        "label_keys": list(event.labels.keys()),
        "label_values": list(event.labels.values()),
    }


def _validate(event: logs_pb2.LogEvent) -> bool:
    """Basic schema validation — rejects events that would corrupt the schema."""
    if not event.service_name:
        return False
    if event.level.upper() not in VALID_LEVELS:
        return False
    return True


class LogCollectorServicer(logs_pb2_grpc.LogCollectorServicer):
    """Implements the LogCollector RPC service."""

    def __init__(self, kafka_producer: KafkaLogProducer):
        self._kafka = kafka_producer

    def CollectBatch(
        self,
        request: logs_pb2.LogBatch,
        context: grpc.ServicerContext,
    ) -> logs_pb2.CollectResponse:
        """Unary RPC: receive a batch, publish valid events to Kafka."""
        accepted = 0
        rejected = 0

        for event in request.events:
            if not _validate(event):
                rejected += 1
                continue
            self._kafka.send(_proto_event_to_dict(event))
            accepted += 1

        batch_id = str(uuid.uuid4())
        logger.info(
            "CollectBatch producer=%s accepted=%d rejected=%d batch_id=%s",
            request.producer_id, accepted, rejected, batch_id,
        )
        return logs_pb2.CollectResponse(
            accepted=accepted,
            rejected=rejected,
            batch_id=batch_id,
        )

    def StreamLogs(
        self,
        request_iterator,
        context: grpc.ServicerContext,
    ) -> logs_pb2.CollectResponse:
        """Client-streaming RPC: high-throughput continuous ingestion."""
        accepted = 0
        rejected = 0

        for event in request_iterator:
            if not _validate(event):
                rejected += 1
                continue
            self._kafka.send(_proto_event_to_dict(event))
            accepted += 1

        self._kafka.flush()
        logger.info("StreamLogs complete accepted=%d rejected=%d", accepted, rejected)
        return logs_pb2.CollectResponse(
            accepted=accepted,
            rejected=rejected,
            batch_id=str(uuid.uuid4()),
        )


def serve(port: int) -> grpc.Server:
    kafka = KafkaLogProducer()
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),  # 50 MB
            ("grpc.max_send_message_length", 10 * 1024 * 1024),
        ],
    )
    logs_pb2_grpc.add_LogCollectorServicer_to_server(
        LogCollectorServicer(kafka), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("gRPC log-collector listening on :%d", port)
    return server
