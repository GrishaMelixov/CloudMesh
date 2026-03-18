package producer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	proto "github.com/GrishaMelixov/CloudMesh/proto"
)

var services = []string{
	"payment-service",
	"auth-service",
	"inventory-service",
	"api-gateway",
	"notification-service",
}

var levels = []string{"DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"}

var messages = []string{
	"request processed successfully",
	"user authenticated",
	"database query executed",
	"cache miss, fetching from db",
	"connection timeout, retrying",
	"invalid request payload",
	"rate limit exceeded",
	"service unavailable",
}

type Generator struct {
	errorRatePct int
	hostname     string
}

func NewGenerator(errorRatePct int, hostname string) *Generator {
	return &Generator{errorRatePct: errorRatePct, hostname: hostname}
}

func (g *Generator) NewBatch(size int) []*proto.LogEvent {
	events := make([]*proto.LogEvent, size)
	for i := range events {
		events[i] = g.newEvent()
	}
	return events
}

func (g *Generator) newEvent() *proto.LogEvent {
	svc := services[rand.Intn(len(services))]
	level := g.pickLevel()
	msg := messages[rand.Intn(len(messages))]
	if level == "ERROR" {
		msg = fmt.Sprintf("error in %s: %s", svc, msg)
	}

	return &proto.LogEvent{
		EventId:     uuid.New().String(),
		ServiceName: svc,
		Host:        g.hostname,
		Level:       level,
		Message:     msg,
		TimestampMs: time.Now().UnixMilli(),
		Labels: map[string]string{
			"trace_id": uuid.New().String()[:8],
			"env":      "production",
		},
	}
}

func (g *Generator) pickLevel() string {
	if rand.Intn(100) < g.errorRatePct {
		return "ERROR"
	}
	return levels[rand.Intn(len(levels))]
}
