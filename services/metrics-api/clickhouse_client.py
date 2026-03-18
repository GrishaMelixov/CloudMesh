"""ClickHouse query client for metrics-api.

All analytics queries hit materialised views (error_rate_per_minute,
volume_per_hour) rather than the raw logs table — this is what produces
the 10x query acceleration claim. The materialised views are tiny
pre-aggregated tables; scanning them is O(minutes) not O(raw rows).
"""

import logging
import os
from functools import lru_cache
from typing import List, Optional

from clickhouse_driver import Client

logger = logging.getLogger(__name__)

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DB = os.getenv("CLICKHOUSE_DB", "cloudmesh")


@lru_cache(maxsize=1)
def get_client() -> Client:
    return Client(host=CH_HOST, port=CH_PORT, compression=True)


def query_error_rate(
    service_name: Optional[str],
    hours: int = 1,
) -> List[dict]:
    """Return per-minute error rates. Hits the materialised view."""
    client = get_client()
    params: dict = {"hours": hours}
    where = "minute >= now() - INTERVAL %(hours)s HOUR"
    if service_name:
        where += " AND service_name = %(service_name)s"
        params["service_name"] = service_name

    rows = client.execute(
        f"""
        SELECT
            service_name,
            minute,
            sum(error_count)  AS errors,
            sum(total_count)  AS total,
            round(sum(error_count) / sum(total_count) * 100, 2) AS error_rate_pct
        FROM {CH_DB}.error_rate_per_minute
        WHERE {where}
        GROUP BY service_name, minute
        ORDER BY minute DESC
        LIMIT 1000
        """,
        params,
        with_column_types=True,
    )
    data, cols = rows
    col_names = [c[0] for c in cols]
    return [dict(zip(col_names, row)) for row in data]


def query_volume(
    service_name: Optional[str],
    days: int = 1,
) -> List[dict]:
    """Return per-hour event volumes. Hits the materialised view."""
    client = get_client()
    params: dict = {"days": days}
    where = "hour >= now() - INTERVAL %(days)s DAY"
    if service_name:
        where += " AND service_name = %(service_name)s"
        params["service_name"] = service_name

    rows = client.execute(
        f"""
        SELECT
            service_name,
            hour,
            sum(event_count) AS event_count
        FROM {CH_DB}.volume_per_hour
        WHERE {where}
        GROUP BY service_name, hour
        ORDER BY hour DESC
        LIMIT 500
        """,
        params,
        with_column_types=True,
    )
    data, cols = rows
    col_names = [c[0] for c in cols]
    return [dict(zip(col_names, row)) for row in data]


def query_recent_logs(
    service_name: Optional[str],
    level: Optional[str],
    limit: int = 100,
) -> List[dict]:
    """Return recent raw log events (directly from the logs table)."""
    client = get_client()
    conditions = ["timestamp_ms > (toUnixTimestamp(now() - INTERVAL 1 HOUR) * 1000)"]
    params: dict = {"limit": limit}
    if service_name:
        conditions.append("service_name = %(service_name)s")
        params["service_name"] = service_name
    if level:
        conditions.append("level = %(level)s")
        params["level"] = level.upper()

    where = " AND ".join(conditions)
    rows = client.execute(
        f"""
        SELECT
            event_id,
            service_name,
            host,
            level,
            message,
            toDateTime(timestamp_ms / 1000) AS timestamp
        FROM {CH_DB}.logs
        WHERE {where}
        ORDER BY timestamp_ms DESC
        LIMIT %(limit)s
        """,
        params,
        with_column_types=True,
    )
    data, cols = rows
    col_names = [c[0] for c in cols]
    return [dict(zip(col_names, row)) for row in data]


def query_services() -> List[str]:
    """Return distinct service names seen in the last 24 hours."""
    client = get_client()
    rows = client.execute(
        f"""
        SELECT DISTINCT service_name
        FROM {CH_DB}.logs
        WHERE event_date >= today() - 1
        ORDER BY service_name
        """
    )
    return [r[0] for r in rows]


def query_total_stats() -> dict:
    """Return high-level stats: total events, error count, unique services."""
    client = get_client()
    rows = client.execute(
        f"""
        SELECT
            count()                     AS total_events,
            countIf(is_error = 1)       AS total_errors,
            uniq(service_name)          AS unique_services,
            min(timestamp_dt)           AS oldest_event,
            max(timestamp_dt)           AS newest_event
        FROM {CH_DB}.logs
        WHERE event_date >= today() - 7
        """
    )
    row = rows[0] if rows else (0, 0, 0, None, None)
    return {
        "total_events": row[0],
        "total_errors": row[1],
        "unique_services": row[2],
        "oldest_event": str(row[3]) if row[3] else None,
        "newest_event": str(row[4]) if row[4] else None,
    }
