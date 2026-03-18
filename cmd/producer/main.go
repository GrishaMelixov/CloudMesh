package main

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/GrishaMelixov/CloudMesh/internal/producer"
)

func main() {
	collectorAddr := getEnv("LOG_COLLECTOR_ADDR", "log-collector:50051")
	eventsPerSec := getEnvInt("EVENTS_PER_SEC", 200)
	batchSize := getEnvInt("BATCH_SIZE", 500)
	errorRatePct := getEnvInt("ERROR_RATE_PCT", 5)
	hostname, _ := os.Hostname()

	client, err := producer.NewClient(collectorAddr)
	if err != nil {
		slog.Error("failed to connect to collector", "addr", collectorAddr, "err", err)
		os.Exit(1)
	}
	defer client.Close()

	gen := producer.NewGenerator(errorRatePct, hostname)
	producerID := uuid.New().String()
	interval := time.Second / time.Duration(eventsPerSec/batchSize+1)

	slog.Info("producer started",
		"collector", collectorAddr,
		"events_per_sec", eventsPerSec,
		"batch_size", batchSize,
		"error_rate_pct", errorRatePct,
	)

	var totalSent uint64
	for {
		events := gen.NewBatch(batchSize)
		accepted, rejected, err := client.SendBatch(producerID, events)
		if err != nil {
			slog.Error("send batch failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		totalSent += uint64(accepted)
		if totalSent%10000 == 0 {
			slog.Info("progress", "total_sent", totalSent, "last_accepted", accepted, "last_rejected", rejected)
		}
		time.Sleep(interval)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
