package main

import (
	"log/slog"
	"os"

	"github.com/GrishaMelixov/CloudMesh/internal/api"
)

func main() {
	addr := getEnv("HTTP_ADDR", ":8080")

	db, err := api.NewDB()
	if err != nil {
		slog.Error("clickhouse init failed", "err", err)
		os.Exit(1)
	}

	router := api.NewRouter(db)

	slog.Info("metrics-api started", "addr", addr)
	if err := router.Run(addr); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
