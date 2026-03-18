"""Kafka producer wrapper for log-collector.

Uses confluent-kafka (librdkafka-backed) for high-throughput delivery.
Events are serialised as JSON for human-readability in the raw topic;
ClickHouse ingestion handles deserialisation on the consumer side.
"""

import json
import logging
import os

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "cloudmesh.logs.raw")


def _delivery_report(err, msg):
    if err:
        logger.error("Kafka delivery failed: %s", err)


class KafkaLogProducer:
    def __init__(self):
        self._producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            # Batching: wait up to 5ms to fill a batch (reduces round-trips)
            "linger.ms": 5,
            # Compress batches with lz4 before sending to broker
            "compression.type": "lz4",
            # Ensure durability: wait for leader acknowledgement
            "acks": "1",
            # Internal send buffer size
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 1048576,  # 1 GB
        })

    def send(self, event_dict: dict) -> None:
        """Publish a single log event to the raw Kafka topic."""
        payload = json.dumps(event_dict, ensure_ascii=False).encode("utf-8")
        # Partition key = service_name → logs from same service go to same partition
        key = event_dict.get("service_name", "unknown").encode("utf-8")
        self._producer.produce(
            topic=TOPIC_RAW,
            key=key,
            value=payload,
            callback=_delivery_report,
        )
        # Non-blocking poll to trigger delivery callbacks without slowing the RPC path
        self._producer.poll(0)

    def flush(self) -> None:
        """Block until all outstanding messages are delivered."""
        self._producer.flush()
