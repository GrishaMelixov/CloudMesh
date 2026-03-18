"""Synthetic log event factory.

Generates realistic-looking log events across multiple services.
Supports a configurable error rate to trigger RabbitMQ alert testing.
"""

import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from faker import Faker

_faker = Faker()

LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "FATAL"]
LEVEL_WEIGHTS = [5, 50, 50, 50, 15, 10, 2]  # INFO is most common

LOG_TEMPLATES = {
    "payment-service": [
        "Payment processed successfully for order {order_id}",
        "Payment failed: insufficient funds for user {user_id}",
        "Stripe webhook received: event_type={event_type}",
        "Refund initiated for transaction {tx_id}",
        "Database connection timeout after {ms}ms",
    ],
    "auth-service": [
        "User {user_id} logged in from {ip}",
        "JWT token issued for user {user_id}, expires in 3600s",
        "Failed login attempt for email {email} — invalid password",
        "Password reset requested for {email}",
        "OAuth2 token exchange completed for provider=google",
    ],
    "inventory-service": [
        "Stock level updated: sku={sku} qty={qty}",
        "Low stock alert: sku={sku} remaining={qty}",
        "Product {product_id} added to catalogue",
        "Warehouse sync completed: {count} items processed",
        "Reserved {qty} units of {sku} for order {order_id}",
    ],
    "api-gateway": [
        "GET /api/v1/products 200 {ms}ms",
        "POST /api/v1/orders 201 {ms}ms",
        "Rate limit exceeded for IP {ip}",
        "Circuit breaker opened for upstream=payment-service",
        "Request routing: {method} {path} → {upstream}",
    ],
    "notification-service": [
        "Email sent to {email}: subject={subject}",
        "Push notification delivered to device {device_id}",
        "SMS gateway timeout for number {phone}",
        "Notification queued: type={notif_type} user={user_id}",
        "Webhook delivery failed after 3 retries: url={url}",
    ],
}

DEFAULT_SERVICES = list(LOG_TEMPLATES.keys())


@dataclass
class LogEvent:
    event_id: str
    service_name: str
    host: str
    level: str
    message: str
    timestamp_ms: int
    labels: dict = field(default_factory=dict)


def _render_template(template: str) -> str:
    return template.format(
        order_id=_faker.bothify("ORD-####-????").upper(),
        user_id=_faker.bothify("USR-########"),
        tx_id=_faker.bothify("TXN-??????????").upper(),
        email=_faker.email(),
        ip=_faker.ipv4(),
        sku=_faker.bothify("SKU-####-??").upper(),
        qty=random.randint(1, 500),
        count=random.randint(100, 10000),
        product_id=_faker.bothify("PROD-####"),
        ms=random.randint(1, 800),
        method=random.choice(["GET", "POST", "PUT", "DELETE"]),
        path=_faker.uri_path(),
        upstream=random.choice(["payment-service", "auth-service", "inventory-service"]),
        event_type=random.choice(["payment_intent.succeeded", "charge.failed", "refund.created"]),
        device_id=_faker.bothify("DEV-??????????"),
        phone=_faker.phone_number(),
        subject=random.choice(["Order confirmation", "Password reset", "Shipment update"]),
        notif_type=random.choice(["email", "push", "sms"]),
        url=_faker.url(),
    )


def generate_event(
    service_name: str,
    error_rate_pct: float = 5.0,
) -> LogEvent:
    """Generate one synthetic log event for the given service."""
    templates = LOG_TEMPLATES.get(service_name, LOG_TEMPLATES["api-gateway"])
    template = random.choice(templates)
    message = _render_template(template)

    # Force ERROR level based on configured rate
    if random.random() < error_rate_pct / 100:
        level = "ERROR"
    else:
        level = random.choices(
            ["DEBUG", "INFO", "WARN", "ERROR"],
            weights=[5, 75, 15, 5],
        )[0]

    return LogEvent(
        event_id=str(uuid.uuid4()),
        service_name=service_name,
        host=f"{service_name}-{_faker.bothify('pod-??##')}",
        level=level,
        message=message,
        timestamp_ms=int(time.time() * 1000),
        labels={
            "trace_id": _faker.bothify("trace-??????????"),
            "env": "production",
        },
    )
