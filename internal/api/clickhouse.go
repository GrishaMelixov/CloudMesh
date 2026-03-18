package api

import (
	"context"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DB struct {
	conn driver.Conn
}

func NewDB() (*DB, error) {
	host := getEnvAPI("CLICKHOUSE_HOST", "clickhouse")
	port := getEnvAPI("CLICKHOUSE_PORT", "9000")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{Database: "cloudmesh"},
	})
	if err != nil {
		return nil, err
	}
	return &DB{conn: conn}, nil
}

type StatsRow struct {
	Total    uint64  `json:"total"`
	Errors   uint64  `json:"errors"`
	Services uint64  `json:"services"`
	ErrorPct float64 `json:"error_pct"`
}

func (db *DB) Stats(ctx context.Context) (*StatsRow, error) {
	row := db.conn.QueryRow(ctx, `
		SELECT
			count()                                        AS total,
			countIf(level IN ('ERROR','FATAL'))            AS errors,
			uniqExact(service_name)                        AS services,
			round(errors / total * 100, 2)                 AS error_pct
		FROM logs
		WHERE timestamp_ms >= toUnixTimestamp(now() - INTERVAL 7 DAY) * 1000
	`)
	var s StatsRow
	if err := row.Scan(&s.Total, &s.Errors, &s.Services, &s.ErrorPct); err != nil {
		return nil, err
	}
	return &s, nil
}

type ServiceRow struct {
	ServiceName string `json:"service_name"`
	Total       uint64 `json:"total"`
	Errors      uint64 `json:"errors"`
}

func (db *DB) Services(ctx context.Context) ([]ServiceRow, error) {
	rows, err := db.conn.Query(ctx, `
		SELECT service_name, count() AS total, countIf(level IN ('ERROR','FATAL')) AS errors
		FROM logs
		WHERE timestamp_ms >= toUnixTimestamp(now() - INTERVAL 24 HOUR) * 1000
		GROUP BY service_name
		ORDER BY total DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ServiceRow
	for rows.Next() {
		var r ServiceRow
		if err := rows.Scan(&r.ServiceName, &r.Total, &r.Errors); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

type ErrorRateRow struct {
	Minute     string  `json:"minute"`
	Service    string  `json:"service"`
	ErrorCount uint64  `json:"error_count"`
	Total      uint64  `json:"total"`
	Rate       float64 `json:"rate"`
}

func (db *DB) ErrorRate(ctx context.Context, hours int) ([]ErrorRateRow, error) {
	rows, err := db.conn.Query(ctx, `
		SELECT
			formatDateTime(toStartOfMinute(fromUnixTimestamp(intDiv(timestamp_ms, 1000))), '%Y-%m-%dT%H:%i') AS minute,
			service_name,
			countIf(level IN ('ERROR','FATAL')) AS error_count,
			count() AS total,
			round(error_count / total * 100, 2) AS rate
		FROM logs
		WHERE timestamp_ms >= toUnixTimestamp(now() - INTERVAL ? HOUR) * 1000
		GROUP BY minute, service_name
		ORDER BY minute DESC
		LIMIT 1000
	`, hours)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ErrorRateRow
	for rows.Next() {
		var r ErrorRateRow
		if err := rows.Scan(&r.Minute, &r.Service, &r.ErrorCount, &r.Total, &r.Rate); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

type VolumeRow struct {
	Hour    string `json:"hour"`
	Service string `json:"service"`
	Count   uint64 `json:"count"`
}

func (db *DB) Volume(ctx context.Context, days int) ([]VolumeRow, error) {
	rows, err := db.conn.Query(ctx, `
		SELECT
			formatDateTime(toStartOfHour(fromUnixTimestamp(intDiv(timestamp_ms, 1000))), '%Y-%m-%dT%H:%i') AS hour,
			service_name,
			count() AS cnt
		FROM logs
		WHERE timestamp_ms >= toUnixTimestamp(now() - INTERVAL ? DAY) * 1000
		GROUP BY hour, service_name
		ORDER BY hour DESC
		LIMIT 1000
	`, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []VolumeRow
	for rows.Next() {
		var r VolumeRow
		if err := rows.Scan(&r.Hour, &r.Service, &r.Count); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

type LogRow struct {
	TimestampMs int64  `json:"timestamp_ms"`
	ServiceName string `json:"service_name"`
	Level       string `json:"level"`
	Host        string `json:"host"`
	Message     string `json:"message"`
}

func (db *DB) RecentLogs(ctx context.Context, service, level string, limit int) ([]LogRow, error) {
	query := `
		SELECT timestamp_ms, service_name, level, host, message
		FROM logs
		WHERE timestamp_ms >= toUnixTimestamp(now() - INTERVAL 1 HOUR) * 1000
	`
	args := []any{}
	if service != "" {
		query += " AND service_name = ?"
		args = append(args, service)
	}
	if level != "" {
		query += " AND level = ?"
		args = append(args, level)
	}
	query += " ORDER BY timestamp_ms DESC LIMIT ?"
	args = append(args, limit)

	rows, err := db.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []LogRow
	for rows.Next() {
		var r LogRow
		if err := rows.Scan(&r.TimestampMs, &r.ServiceName, &r.Level, &r.Host, &r.Message); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

func (db *DB) Ping(ctx context.Context) error {
	return db.conn.Ping(ctx)
}

func getEnvAPI(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
