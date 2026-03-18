"""Unit tests for metrics-api FastAPI endpoints.

Uses FastAPI TestClient — no real ClickHouse needed (queries are mocked).
"""

import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../services/metrics-api"))

pytest.importorskip("fastapi", reason="fastapi not installed")

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

MOCK_STATS = {
    "total_events": 1500000,
    "total_errors": 75000,
    "unique_services": 5,
    "oldest_event": "2026-03-10 00:00:00",
    "newest_event": "2026-03-18 12:00:00",
}

MOCK_ERROR_RATE = [
    {"service_name": "payment-service", "minute": datetime(2026, 3, 18, 12, 0), "errors": 12, "total": 100, "error_rate_pct": 12.0},
    {"service_name": "payment-service", "minute": datetime(2026, 3, 18, 12, 1), "errors": 5, "total": 100, "error_rate_pct": 5.0},
]

MOCK_VOLUME = [
    {"service_name": "payment-service", "hour": datetime(2026, 3, 18, 12, 0), "event_count": 72000},
]

MOCK_LOGS = [
    {"event_id": "abc-123", "service_name": "auth-service", "host": "pod-01",
     "level": "ERROR", "message": "login failed", "timestamp": datetime(2026, 3, 18, 12, 0)},
]


def test_root_returns_service_info():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "metrics-api"
    assert "/docs" in data["docs"]


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@patch("routers.analytics.query_total_stats", return_value=MOCK_STATS)
def test_stats_endpoint(mock_stats):
    response = client.get("/analytics/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["total_events"] == 1500000
    assert data["unique_services"] == 5


@patch("routers.analytics.query_error_rate", return_value=MOCK_ERROR_RATE)
def test_error_rate_no_filter(mock_fn):
    response = client.get("/analytics/error-rate")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2


@patch("routers.analytics.query_error_rate", return_value=MOCK_ERROR_RATE)
def test_error_rate_with_service_filter(mock_fn):
    response = client.get("/analytics/error-rate?service=payment-service&hours=6")
    assert response.status_code == 200
    mock_fn.assert_called_once_with(service_name="payment-service", hours=6)


@patch("routers.analytics.query_volume", return_value=MOCK_VOLUME)
def test_volume_endpoint(mock_fn):
    response = client.get("/analytics/volume?days=7")
    assert response.status_code == 200
    mock_fn.assert_called_once_with(service_name=None, days=7)


@patch("routers.analytics.query_recent_logs", return_value=MOCK_LOGS)
def test_logs_endpoint_with_filters(mock_fn):
    response = client.get("/analytics/logs?service=auth-service&level=ERROR&limit=50")
    assert response.status_code == 200
    mock_fn.assert_called_once_with(service_name="auth-service", level="ERROR", limit=50)


def test_logs_limit_validation():
    response = client.get("/analytics/logs?limit=9999")
    assert response.status_code == 422  # FastAPI validation: limit > 1000


@patch("routers.analytics.query_services", return_value=["payment-service", "auth-service"])
def test_services_endpoint(mock_fn):
    response = client.get("/analytics/services")
    assert response.status_code == 200
    assert "payment-service" in response.json()
