"""log-collector entry point.

Starts two servers concurrently:
  1. gRPC server on GRPC_PORT (primary, high-throughput path)
  2. HTTP REST shim on HTTP_PORT (for benchmark comparison only)
"""

import logging
import os
import threading
import time

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
HTTP_PORT = int(os.getenv("HTTP_PORT", "50052"))


# ── HTTP REST shim (benchmark comparison only) ────────────────────────────────

class IngestRequest(BaseModel):
    service_name: str
    level: str = "INFO"
    message: str
    timestamp_ms: int = 0


rest_app = FastAPI(title="log-collector REST shim", docs_url=None, redoc_url=None)

# Lazy-initialised Kafka producer shared with the HTTP path
_kafka_producer = None


def get_kafka():
    global _kafka_producer
    if _kafka_producer is None:
        from kafka_producer import KafkaLogProducer
        _kafka_producer = KafkaLogProducer()
    return _kafka_producer


@rest_app.post("/ingest")
def ingest(req: IngestRequest):
    """Single-event HTTP endpoint — exists purely for benchmark comparison."""
    import time as _time
    import uuid
    get_kafka().send({
        "event_id": str(uuid.uuid4()),
        "service_name": req.service_name,
        "host": "rest-shim",
        "level": req.level.upper(),
        "message": req.message,
        "timestamp_ms": req.timestamp_ms or int(_time.time() * 1000),
        "label_keys": [],
        "label_values": [],
    })
    return {"status": "ok"}


@rest_app.get("/health")
def health():
    return {"status": "ok", "service": "log-collector"}


# ── Main ──────────────────────────────────────────────────────────────────────

def start_grpc():
    from grpc_server import serve
    server = serve(GRPC_PORT)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5)


def start_http():
    uvicorn.run(rest_app, host="0.0.0.0", port=HTTP_PORT, log_level="warning")


if __name__ == "__main__":
    grpc_thread = threading.Thread(target=start_grpc, daemon=True)
    grpc_thread.start()

    logger.info("HTTP REST shim starting on :%d", HTTP_PORT)
    start_http()
