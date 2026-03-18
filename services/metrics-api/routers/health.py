"""Health check endpoints."""

import os

from fastapi import APIRouter
from clickhouse_driver import Client

router = APIRouter(tags=["health"])

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))


@router.get("/health")
def health():
    """Basic liveness probe."""
    return {"status": "ok", "service": "metrics-api"}


@router.get("/health/ready")
def readiness():
    """Readiness probe: checks ClickHouse connectivity."""
    try:
        client = Client(host=CH_HOST, port=CH_PORT)
        client.execute("SELECT 1")
        return {"status": "ready", "clickhouse": "ok"}
    except Exception as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")
