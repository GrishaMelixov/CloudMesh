"""pytest configuration for CloudMesh tests.

Markers:
  - integration: tests that require external services (ClickHouse, Kafka, RabbitMQ)
                 run with: pytest -k "integration"
                 skip with: pytest -k "not integration" (default in CI unit job)
"""

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as requiring running infrastructure"
    )
