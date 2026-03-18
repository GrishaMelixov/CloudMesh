package producer

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	proto "github.com/GrishaMelixov/CloudMesh/proto"
)

type Client struct {
	conn   *grpc.ClientConn
	client proto.LogCollectorClient
}

func NewClient(addr string) (*Client, error) {
	var conn *grpc.ClientConn
	var err error

	// retry until collector is up
	for i := range 30 {
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		slog.Info("waiting for collector", "attempt", i+1, "addr", addr)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		client: proto.NewLogCollectorClient(conn),
	}, nil
}

func (c *Client) SendBatch(producerID string, events []*proto.LogEvent) (accepted, rejected uint32, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.CollectBatch(ctx, &proto.LogBatch{
		ProducerId: producerID,
		Events:     events,
	})
	if err != nil {
		return 0, 0, err
	}
	return resp.Accepted, resp.Rejected, nil
}

func (c *Client) Close() {
	c.conn.Close()
}
