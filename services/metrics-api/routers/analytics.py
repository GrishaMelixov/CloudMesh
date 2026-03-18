"""Analytics endpoints.

All heavy queries hit ClickHouse materialised views — the pre-aggregated
tables are tiny, so responses are fast even over large datasets.
This is what backs the "10x faster search" claim in the resume.
"""

from typing import List, Optional

from fastapi import APIRouter, Query

from clickhouse_client import (
    query_error_rate,
    query_volume,
    query_recent_logs,
    query_services,
    query_total_stats,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/stats")
def stats():
    """Overall system stats for the last 7 days."""
    return query_total_stats()


@router.get("/services")
def services():
    """List distinct service names seen in the last 24 hours."""
    return query_services()


@router.get("/error-rate")
def error_rate(
    service: Optional[str] = Query(None, description="Filter by service name"),
    hours: int = Query(1, ge=1, le=168, description="Lookback window in hours"),
):
    """
    Per-minute error rates from the materialised view.
    Hits error_rate_per_minute — does NOT scan raw logs.
    """
    return query_error_rate(service_name=service, hours=hours)


@router.get("/volume")
def volume(
    service: Optional[str] = Query(None, description="Filter by service name"),
    days: int = Query(1, ge=1, le=30, description="Lookback window in days"),
):
    """
    Per-hour event volume from the materialised view.
    Hits volume_per_hour — does NOT scan raw logs.
    """
    return query_volume(service_name=service, days=days)


@router.get("/logs")
def recent_logs(
    service: Optional[str] = Query(None),
    level: Optional[str] = Query(None, description="Filter by level (INFO, ERROR, etc.)"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Recent log events from the raw logs table (last 1 hour)."""
    return query_recent_logs(service_name=service, level=level, limit=limit)
