package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/GrishaMelixov/CloudMesh/internal/aggregator"
)

func main() {
	thresholdPct := getEnvFloat("ERROR_RATE_THRESHOLD", 10.0)
	windowSec := getEnvInt("ERROR_RATE_WINDOW_SEC", 60)

	chWriter, err := aggregator.NewClickHouseWriter()
	if err != nil {
		slog.Error("clickhouse init failed", "err", err)
		os.Exit(1)
	}
	defer chWriter.Close()

	alerter, err := aggregator.NewAlerter()
	if err != nil {
		slog.Error("rabbitmq init failed", "err", err)
		os.Exit(1)
	}
	defer alerter.Close()

	monitor := aggregator.NewErrorRateMonitor(windowSec, thresholdPct)
	monitor.OnAlert = func(service string, rate float64) {
		slog.Warn("error rate spike", "service", service, "rate_pct", rate)
		alerter.SendErrorRateAlert(service, rate, thresholdPct)
	}

	consumer := aggregator.NewConsumer(chWriter, monitor, alerter)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("aggregator started", "error_threshold_pct", thresholdPct, "window_sec", windowSec)
	consumer.Run(ctx)
	slog.Info("aggregator stopped")
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
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
