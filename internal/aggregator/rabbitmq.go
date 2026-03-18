package aggregator

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const exchange = "cloudmesh.alerts"

type Alerter struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewAlerter() (*Alerter, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@rabbitmq:5672/"
	}

	var conn *amqp.Connection
	var err error
	for i := range 30 {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		slog.Info("waiting for rabbitmq", "attempt", i+1)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("rabbitmq connect: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}

	// declare queues and bind routing keys
	queues := []struct {
		name string
		key  string
	}{
		{"q.alerts.error_rate", "alert.error_rate.#"},
		{"q.alerts.volume_spike", "alert.volume_spike.#"},
		{"q.alerts.all", "alert.#"},
	}
	for _, q := range queues {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, nil); err != nil {
			return nil, err
		}
		if err := ch.QueueBind(q.name, q.key, exchange, false, nil); err != nil {
			return nil, err
		}
	}

	return &Alerter{conn: conn, channel: ch}, nil
}

func (a *Alerter) SendErrorRateAlert(service string, rate, threshold float64) {
	payload := map[string]any{
		"alert_type":    "error_rate_spike",
		"service_name":  service,
		"error_rate_pct": rate,
		"threshold_pct": threshold,
		"triggered_at":  time.Now().UTC().Format(time.RFC3339),
	}
	a.publish(fmt.Sprintf("alert.error_rate.%s", service), payload)
}

func (a *Alerter) publish(routingKey string, payload map[string]any) {
	body, _ := json.Marshal(payload)
	err := a.channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		slog.Error("rabbitmq publish failed", "key", routingKey, "err", err)
		return
	}
	slog.Info("alert sent", "key", routingKey)
}

func (a *Alerter) Close() {
	a.channel.Close()
	a.conn.Close()
}
