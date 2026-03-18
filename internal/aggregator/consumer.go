package aggregator

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

type rawEvent struct {
	EventID     string            `json:"event_id"`
	ServiceName string            `json:"service_name"`
	Host        string            `json:"host"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	TimestampMs int64             `json:"timestamp_ms"`
	Labels      map[string]string `json:"labels"`
}

type Consumer struct {
	reader  *kafka.Reader
	writer  *ClickHouseWriter
	monitor *ErrorRateMonitor
	alerter *Alerter
}

func NewConsumer(writer *ClickHouseWriter, monitor *ErrorRateMonitor, alerter *Alerter) *Consumer {
	brokers := getEnvAgg("KAFKA_BROKERS", "kafka:9092")
	topic := getEnvAgg("KAFKA_TOPIC_RAW", "cloudmesh.logs.raw")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    topic,
		GroupID:  "clickhouse-writers",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &Consumer{
		reader:  r,
		writer:  writer,
		monitor: monitor,
		alerter: alerter,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	slog.Info("aggregator consumer started")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("kafka fetch", "err", err)
			continue
		}

		var ev rawEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			slog.Warn("bad message", "err", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		keys, vals := labelsToSlices(ev.Labels)
		c.writer.Add(LogRow{
			EventID:     ev.EventID,
			ServiceName: ev.ServiceName,
			Host:        ev.Host,
			Level:       ev.Level,
			Message:     ev.Message,
			TimestampMs: ev.TimestampMs,
			LabelKeys:   keys,
			LabelValues: vals,
		})

		isError := strings.ToUpper(ev.Level) == "ERROR" || strings.ToUpper(ev.Level) == "FATAL"
		c.monitor.Record(ev.ServiceName, isError)

		_ = c.reader.CommitMessages(ctx, msg)
	}
}

func (c *Consumer) Close() {
	c.reader.Close()
}

func labelsToSlices(m map[string]string) ([]string, []string) {
	keys := make([]string, 0, len(m))
	vals := make([]string, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals
}

func getEnvAgg(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
