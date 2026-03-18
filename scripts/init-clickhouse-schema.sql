-- CloudMesh ClickHouse Schema
-- OLAP-optimised for log analytics
-- Run once on startup via docker-entrypoint-initdb.d

CREATE DATABASE IF NOT EXISTS cloudmesh;

-- ─────────────────────────────────────────────────────────────────
-- Main log table
-- MergeTree family: columnar storage, sorted indices, partitioning
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cloudmesh.logs
(
    -- Temporal columns
    event_date   Date        MATERIALIZED toDate(toDateTime(timestamp_ms / 1000)),
    timestamp_ms Int64,
    timestamp_dt DateTime    MATERIALIZED toDateTime(timestamp_ms / 1000),

    -- Identity
    event_id     String,
    service_name LowCardinality(String),  -- dict-encoded, ~100 distinct values
    host         LowCardinality(String),
    level        LowCardinality(String),  -- only 5 values: major storage saving

    -- Payload
    message      String,

    -- Computed
    is_error     UInt8 MATERIALIZED (level IN ('ERROR', 'FATAL') ? 1 : 0),

    -- Labels stored as parallel arrays (faster GROUP BY than Map type)
    label_keys   Array(String),
    label_values Array(String)
)
ENGINE = MergeTree()
-- Partition by month + service keeps partition files small and enables
-- whole-partition pruning for time-range + service queries
PARTITION BY (toYYYYMM(event_date), service_name)
-- Primary sort index matches the most common query: "errors for service X in range T"
ORDER BY (service_name, level, timestamp_ms)
SETTINGS index_granularity = 8192;

-- Auto-drop logs older than 90 days (keeps storage bounded in production)
ALTER TABLE cloudmesh.logs
    MODIFY TTL event_date + INTERVAL 90 DAY;

-- ─────────────────────────────────────────────────────────────────
-- Materialised view: error counts per minute per service
-- Answers /analytics/error-rate without scanning raw logs at all
-- ─────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS cloudmesh.error_rate_per_minute
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (service_name, minute)
POPULATE
AS
SELECT
    service_name,
    toStartOfMinute(timestamp_dt) AS minute,
    countIf(is_error = 1)         AS error_count,
    count()                       AS total_count
FROM cloudmesh.logs
GROUP BY service_name, minute;

-- ─────────────────────────────────────────────────────────────────
-- Materialised view: event volume per hour per service
-- Answers /analytics/volume instantly
-- ─────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS cloudmesh.volume_per_hour
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (service_name, hour)
POPULATE
AS
SELECT
    service_name,
    toStartOfHour(timestamp_dt) AS hour,
    count()                     AS event_count
FROM cloudmesh.logs
GROUP BY service_name, hour;
