"""log-aggregator entry point.

Consumes log events from Kafka, writes them to ClickHouse in batches,
and triggers RabbitMQ alerts when error rates exceed the configured threshold.

Consumer group: clickhouse-writers
  - 1 consumer per partition (up to 12 parallel instances via K8s HPA)
  - Kafka guarantees at-least-once delivery; ClickHouse handles duplicates
    via the ReplacingMergeTree variant (future optimisation) or by idempotent
    event_id-based dedup queries
"""

import json
import logging
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "cloudmesh.logs.raw")
KAFKA_TOPIC_ERRORS = os.getenv("KAFKA_TOPIC_ERRORS", "cloudmesh.logs.errors")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickhouse-writers")
ERROR_RATE_THRESHOLD = float(os.getenv("ERROR_RATE_THRESHOLD_PCT", "10"))
ERROR_RATE_WINDOW = int(os.getenv("ERROR_RATE_WINDOW_SECONDS", "60"))

# Debounce: don't fire alerts more often than once per minute per service
_last_alert: dict[str, float] = {}
ALERT_DEBOUNCE_SECONDS = 60


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
        # Commit offsets manually after successful ClickHouse write
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
    })


def wait_for_kafka(max_retries: int = 30):
    consumer = create_consumer()
    for attempt in range(max_retries):
        try:
            meta = consumer.list_topics(timeout=5)
            if KAFKA_TOPIC_RAW in meta.topics:
                consumer.close()
                logger.info("Kafka is ready, topic %s found", KAFKA_TOPIC_RAW)
                return
        except Exception as exc:
            logger.info("Waiting for Kafka... attempt %d/%d (%s)", attempt + 1, max_retries, exc)
        time.sleep(3)
    logger.error("Kafka not ready after %d attempts — exiting", max_retries)
    sys.exit(1)


def main():
    wait_for_kafka()

    from clickhouse_writer import ClickHouseWriter
    from error_rate_monitor import ErrorRateMonitor
    from rabbitmq_alerter import RabbitMQAlerter

    alerter = RabbitMQAlerter()
    writer = ClickHouseWriter()

    def on_error_rate_alert(service_name: str, error_rate: float, event_count: int):
        now = time.time()
        last = _last_alert.get(service_name, 0)
        if now - last < ALERT_DEBOUNCE_SECONDS:
            return  # Debounce: suppress duplicate alerts
        _last_alert[service_name] = now
        alerter.publish_error_rate_alert(
            service_name=service_name,
            error_rate_pct=error_rate,
            threshold_pct=ERROR_RATE_THRESHOLD,
            event_count=event_count,
            window_seconds=ERROR_RATE_WINDOW,
        )

    monitor = ErrorRateMonitor(
        window_seconds=ERROR_RATE_WINDOW,
        threshold_pct=ERROR_RATE_THRESHOLD,
        on_alert=on_error_rate_alert,
    )

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC_RAW])

    # Also publish errors to the dedicated errors topic
    from confluent_kafka import Producer as KafkaProducer
    errors_producer = KafkaProducer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    total_consumed = 0
    logger.info("Aggregator consuming from %s (group=%s)", KAFKA_TOPIC_RAW, KAFKA_GROUP_ID)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as exc:
                logger.warning("Malformed message: %s", exc)
                consumer.commit(msg)
                continue

            writer.add(event)
            monitor.record(event.get("service_name", "unknown"), event.get("level", "INFO"))

            # Mirror errors to the dedicated errors topic
            if event.get("level", "").upper() in ("ERROR", "FATAL"):
                errors_producer.produce(
                    topic=KAFKA_TOPIC_ERRORS,
                    key=msg.key(),
                    value=msg.value(),
                )
                errors_producer.poll(0)

            consumer.commit(msg)
            total_consumed += 1

            if total_consumed % 10000 == 0:
                logger.info("Consumed %d events total", total_consumed)

    finally:
        writer.flush_and_stop()
        errors_producer.flush()
        consumer.close()
        alerter.close()
        logger.info("Aggregator stopped. Total consumed: %d", total_consumed)


if __name__ == "__main__":
    main()
