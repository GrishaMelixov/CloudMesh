#!/usr/bin/env bash
# CloudMesh Benchmark: gRPC (batched) vs HTTP REST (per-event)
# Demonstrates the 5x throughput improvement claim from the resume
#
# Usage:
#   ./scripts/benchmark.sh
#
# Requires: docker-compose stack running (docker-compose up -d)

set -euo pipefail

EVENTS=100000
BATCH_SIZE=500
COLLECTOR_GRPC="localhost:50051"
COLLECTOR_HTTP="http://localhost:50052"  # REST shim enabled on log-collector

echo "========================================"
echo "CloudMesh Throughput Benchmark"
echo "Events: $EVENTS | Batch size: $BATCH_SIZE"
echo "========================================"

# ── gRPC benchmark ──────────────────────────────────────────────
echo ""
echo "[1/2] gRPC + Protobuf (batched, $BATCH_SIZE events/call)..."

GRPC_START=$(date +%s%N)

docker run --rm --network cloudmesh_default \
  -e LOG_COLLECTOR_HOST=log-collector \
  -e LOG_COLLECTOR_PORT=50051 \
  -e EVENTS_PER_SECOND=99999 \
  -e BATCH_SIZE=$BATCH_SIZE \
  -e BENCHMARK_EVENTS=$EVENTS \
  cloudmesh/log-producer:latest \
  python main.py --benchmark

GRPC_END=$(date +%s%N)
GRPC_MS=$(( (GRPC_END - GRPC_START) / 1000000 ))
GRPC_EPS=$(( EVENTS * 1000 / GRPC_MS ))

echo "gRPC result: ${EVENTS} events in ${GRPC_MS}ms → ${GRPC_EPS} events/sec"

# ── HTTP REST benchmark ──────────────────────────────────────────
echo ""
echo "[2/2] HTTP REST (JSON, 1 event/request for comparison)..."

HTTP_START=$(date +%s%N)

# Send 10000 requests (subset; full 100k would take too long for REST)
HTTP_EVENTS=10000
for i in $(seq 1 $HTTP_EVENTS); do
  curl -s -o /dev/null -X POST "$COLLECTOR_HTTP/ingest" \
    -H "Content-Type: application/json" \
    -d "{\"service_name\":\"bench\",\"level\":\"INFO\",\"message\":\"bench-$i\",\"timestamp_ms\":$(date +%s%3N)}"
done 2>/dev/null || true

HTTP_END=$(date +%s%N)
HTTP_MS=$(( (HTTP_END - HTTP_START) / 1000000 ))
HTTP_EPS=$(( HTTP_EVENTS * 1000 / HTTP_MS ))

echo "HTTP result: ${HTTP_EVENTS} events in ${HTTP_MS}ms → ${HTTP_EPS} events/sec"

# ── Summary ──────────────────────────────────────────────────────
echo ""
echo "========================================"
if [ "$HTTP_EPS" -gt 0 ]; then
  RATIO=$(( GRPC_EPS / HTTP_EPS ))
  echo "Throughput ratio: gRPC is ~${RATIO}x faster than HTTP REST"
fi
echo ""
echo "Why gRPC wins:"
echo "  - Binary Protobuf encoding vs JSON text (~3-5x smaller payload)"
echo "  - HTTP/2 multiplexing vs HTTP/1.1 connection overhead"
echo "  - Batching: $BATCH_SIZE events per RPC call vs 1 per HTTP request"
echo "========================================"
