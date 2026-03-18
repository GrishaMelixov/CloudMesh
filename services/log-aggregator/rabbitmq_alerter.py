"""RabbitMQ alert publisher.

Publishes structured alert messages to the cloudmesh.alerts topic exchange.
Uses a topic exchange so downstream consumers can subscribe to specific
service alerts (routing key: alert.error_rate.<service_name>).

RabbitMQ is used for alerts (not Kafka) because:
  - Low volume, routing-heavy traffic → topic exchange is the right tool
  - Acknowledgement-driven delivery ensures no alert is silently dropped
  - Separates concern: Kafka handles high-throughput logs, RabbitMQ handles alerts
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pika
import pika.exceptions

logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "cloudmesh.alerts")

QUEUES = [
    ("q.alerts.error_rate", "alert.error_rate.#"),
    ("q.alerts.volume_spike", "alert.volume_spike.#"),
    ("q.alerts.all", "alert.#"),
]


class RabbitMQAlerter:
    def __init__(self):
        self._connection = None
        self._channel = None
        self._connect()

    def _connect(self, retries: int = 10):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=30,
        )
        for attempt in range(retries):
            try:
                self._connection = pika.BlockingConnection(params)
                self._channel = self._connection.channel()
                self._setup_topology()
                logger.info("Connected to RabbitMQ at %s:%d", RABBITMQ_HOST, RABBITMQ_PORT)
                return
            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning("RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
                time.sleep(3)
        raise RuntimeError("Could not connect to RabbitMQ after retries")

    def _setup_topology(self):
        """Declare exchange and queues (idempotent)."""
        self._channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )
        for queue_name, routing_key in QUEUES:
            self._channel.queue_declare(queue=queue_name, durable=True)
            self._channel.queue_bind(
                exchange=EXCHANGE,
                queue=queue_name,
                routing_key=routing_key,
            )

    def publish_error_rate_alert(
        self,
        service_name: str,
        error_rate_pct: float,
        threshold_pct: float,
        event_count: int,
        window_seconds: int,
    ) -> None:
        """Publish an error rate alert for a specific service."""
        body = json.dumps({
            "alert_type": "error_rate_spike",
            "service_name": service_name,
            "error_rate_pct": round(error_rate_pct, 2),
            "threshold_pct": threshold_pct,
            "window_seconds": window_seconds,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "event_count": event_count,
        })
        routing_key = f"alert.error_rate.{service_name}"
        try:
            self._channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=body.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=2,      # Persistent — survives broker restart
                    content_type="application/json",
                ),
            )
            logger.info("Published alert: %s error_rate=%.1f%%", routing_key, error_rate_pct)
        except pika.exceptions.AMQPError as exc:
            logger.error("Failed to publish alert: %s", exc)
            self._reconnect()

    def _reconnect(self):
        logger.info("Reconnecting to RabbitMQ...")
        try:
            self._connect()
        except RuntimeError:
            logger.error("Reconnect failed — alerts may be lost")

    def close(self):
        if self._connection and not self._connection.is_closed:
            self._connection.close()
