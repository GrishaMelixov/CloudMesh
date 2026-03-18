package collector

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	proto "github.com/GrishaMelixov/CloudMesh/proto"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer() *KafkaProducer {
	brokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	topic := getEnv("KAFKA_TOPIC_RAW", "cloudmesh.logs.raw")

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		Compression:  kafka.Lz4,
		RequiredAcks: kafka.RequireOne,
	}
	return &KafkaProducer{writer: w}
}

func (p *KafkaProducer) Send(ev *proto.LogEvent) error {
	data, err := json.Marshal(map[string]any{
		"event_id":     ev.EventId,
		"service_name": ev.ServiceName,
		"host":         ev.Host,
		"level":        ev.Level,
		"message":      ev.Message,
		"timestamp_ms": ev.TimestampMs,
		"labels":       ev.Labels,
	})
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(ev.ServiceName),
		Value: data,
	})
}

func (p *KafkaProducer) Close() {
	p.writer.Close()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
