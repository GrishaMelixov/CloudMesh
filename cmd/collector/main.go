package main

import (
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
	proto "github.com/GrishaMelixov/CloudMesh/proto"
	"github.com/GrishaMelixov/CloudMesh/internal/collector"
)

func main() {
	addr := getEnv("GRPC_ADDR", ":50051")

	producer := collector.NewKafkaProducer()
	defer producer.Close()

	srv := collector.NewServer(producer)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "addr", addr, "err", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	proto.RegisterLogCollectorServer(s, srv)

	slog.Info("log-collector started", "addr", addr)
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
