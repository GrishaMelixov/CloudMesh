package aggregator

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	flushSize    = 5000
	flushTimeout = 5 * time.Second
)

type LogRow struct {
	ServiceName string
	Host        string
	Level       string
	Message     string
	TimestampMs int64
	EventID     string
	LabelKeys   []string
	LabelValues []string
}

type ClickHouseWriter struct {
	conn   driver.Conn
	mu     sync.Mutex
	buf    []LogRow
	ticker *time.Ticker
	done   chan struct{}
}

func NewClickHouseWriter() (*ClickHouseWriter, error) {
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		dsn = "clickhouse://default:@clickhouse:9000/cloudmesh"
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{getHost(dsn)},
		Auth: clickhouse.Auth{Database: "cloudmesh"},
	})
	if err != nil {
		return nil, err
	}

	w := &ClickHouseWriter{
		conn:   conn,
		buf:    make([]LogRow, 0, flushSize),
		ticker: time.NewTicker(flushTimeout),
		done:   make(chan struct{}),
	}

	go w.flushLoop()
	return w, nil
}

func (w *ClickHouseWriter) Add(row LogRow) {
	w.mu.Lock()
	w.buf = append(w.buf, row)
	shouldFlush := len(w.buf) >= flushSize
	w.mu.Unlock()

	if shouldFlush {
		w.Flush()
	}
}

func (w *ClickHouseWriter) flushLoop() {
	for {
		select {
		case <-w.ticker.C:
			w.Flush()
		case <-w.done:
			w.Flush()
			return
		}
	}
}

func (w *ClickHouseWriter) Flush() {
	w.mu.Lock()
	if len(w.buf) == 0 {
		w.mu.Unlock()
		return
	}
	rows := w.buf
	w.buf = make([]LogRow, 0, flushSize)
	w.mu.Unlock()

	ctx := context.Background()
	// Column order must match DDL (excluding MATERIALIZED columns):
	// timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values
	batch, err := w.conn.PrepareBatch(ctx,
		"INSERT INTO logs (timestamp_ms, event_id, service_name, host, level, message, label_keys, label_values)",
	)
	if err != nil {
		slog.Error("clickhouse prepare batch", "err", err)
		return
	}

	for _, r := range rows {
		if err := batch.Append(
			r.TimestampMs,
			r.EventID,
			r.ServiceName,
			r.Host,
			r.Level,
			r.Message,
			r.LabelKeys,
			r.LabelValues,
		); err != nil {
			slog.Error("clickhouse append row", "err", err)
		}
	}

	if err := batch.Send(); err != nil {
		slog.Error("clickhouse send batch", "err", err, "rows", len(rows))
		return
	}

	slog.Info("flushed to clickhouse", "rows", len(rows))
}

func (w *ClickHouseWriter) Close() {
	w.ticker.Stop()
	close(w.done)
}

// getHost extracts host:port from a clickhouse DSN
func getHost(dsn string) string {
	// e.g. "clickhouse://default:@clickhouse:9000/cloudmesh" -> "clickhouse:9000"
	host := os.Getenv("CLICKHOUSE_HOST")
	port := os.Getenv("CLICKHOUSE_PORT")
	if host == "" {
		host = "clickhouse"
	}
	if port == "" {
		port = "9000"
	}
	return host + ":" + port
}
