package collector

import (
	"context"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	proto "github.com/GrishaMelixov/CloudMesh/proto"
)

var validLevels = map[string]bool{
	"DEBUG": true, "INFO": true, "WARN": true,
	"WARNING": true, "ERROR": true, "FATAL": true,
}

type Server struct {
	proto.UnimplementedLogCollectorServer
	producer *KafkaProducer
}

func NewServer(p *KafkaProducer) *Server {
	return &Server{producer: p}
}

func (s *Server) CollectBatch(_ context.Context, batch *proto.LogBatch) (*proto.CollectResponse, error) {
	batchID := uuid.New().String()
	var accepted, rejected uint32

	for _, ev := range batch.Events {
		if !isValid(ev) {
			rejected++
			continue
		}
		if err := s.producer.Send(ev); err != nil {
			slog.Error("kafka send failed", "err", err)
			rejected++
		} else {
			accepted++
		}
	}

	slog.Info("batch collected", "batch_id", batchID, "accepted", accepted, "rejected", rejected)
	return &proto.CollectResponse{
		BatchId:  batchID,
		Accepted: accepted,
		Rejected: rejected,
	}, nil
}

func (s *Server) StreamLogs(stream proto.LogCollector_StreamLogsServer) error {
	batchID := uuid.New().String()
	var accepted, rejected uint32

	for {
		ev, err := stream.Recv()
		if err != nil {
			break
		}
		if !isValid(ev) {
			rejected++
			continue
		}
		if err := s.producer.Send(ev); err != nil {
			slog.Error("kafka send failed", "err", err)
			rejected++
		} else {
			accepted++
		}
	}

	return stream.SendAndClose(&proto.CollectResponse{
		BatchId:  batchID,
		Accepted: accepted,
		Rejected: rejected,
	})
}

func isValid(ev *proto.LogEvent) bool {
	return ev.ServiceName != "" && validLevels[strings.ToUpper(ev.Level)]
}
