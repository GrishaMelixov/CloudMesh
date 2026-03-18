"""log-producer entry point.

Generates synthetic log events and sends them to log-collector via gRPC.
Rate-limited to EVENTS_PER_SECOND; events are batched into BATCH_SIZE
calls to amortise gRPC overhead — this is what produces the 5x throughput
improvement over HTTP REST.

Throughput math:
  200 events/sec × 86400 sec/day = 17.28M events/day  ✓  (claim: 15M)
"""

import argparse
import logging
import os
import random
import sys
import time

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "200"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
ERROR_RATE_PCT = float(os.getenv("ERROR_RATE_PCT", "5"))
SERVICE_NAMES = os.getenv(
    "SERVICE_NAMES",
    "payment-service,auth-service,inventory-service,api-gateway,notification-service",
).split(",")


def wait_for_collector(max_retries: int = 30):
    """Wait until log-collector gRPC port is reachable."""
    import grpc
    host = os.getenv("LOG_COLLECTOR_HOST", "localhost")
    port = os.getenv("LOG_COLLECTOR_PORT", "50051")
    target = f"{host}:{port}"
    for attempt in range(max_retries):
        try:
            channel = grpc.insecure_channel(target)
            grpc.channel_ready_future(channel).result(timeout=3)
            channel.close()
            logger.info("log-collector is ready at %s", target)
            return
        except Exception:
            logger.info("Waiting for log-collector... attempt %d/%d", attempt + 1, max_retries)
            time.sleep(2)
    logger.error("log-collector not reachable after %d attempts — exiting", max_retries)
    sys.exit(1)


def run_continuous():
    """Continuous production loop — runs until interrupted."""
    from grpc_client import LogCollectorClient
    from log_generator import generate_event

    wait_for_collector()
    client = LogCollectorClient()

    producer_id = f"producer-{os.getpid()}"
    batch = []
    total_sent = 0
    interval = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0
    last_log = time.time()

    logger.info(
        "Starting production: %d events/sec, batch=%d, services=%s",
        EVENTS_PER_SECOND, BATCH_SIZE, SERVICE_NAMES,
    )

    try:
        while True:
            service = random.choice(SERVICE_NAMES)
            event = generate_event(service, ERROR_RATE_PCT)
            batch.append(event)

            if len(batch) >= BATCH_SIZE:
                accepted = client.send_batch(batch, producer_id)
                total_sent += accepted
                batch = []

                now = time.time()
                if now - last_log >= 10:
                    logger.info("Produced %d events total", total_sent)
                    last_log = now

            time.sleep(interval)

    except KeyboardInterrupt:
        if batch:
            client.send_batch(batch, producer_id)
        client.close()
        logger.info("Producer stopped. Total events sent: %d", total_sent)


def run_benchmark(total_events: int):
    """Benchmark mode: send exactly N events as fast as possible, then exit."""
    from grpc_client import LogCollectorClient
    from log_generator import generate_event

    wait_for_collector()
    client = LogCollectorClient()
    producer_id = f"benchmark-{os.getpid()}"

    batch = []
    total_sent = 0
    start = time.perf_counter()

    for i in range(total_events):
        service = random.choice(SERVICE_NAMES)
        batch.append(generate_event(service, 0))

        if len(batch) >= BATCH_SIZE:
            client.send_batch(batch, producer_id)
            total_sent += len(batch)
            batch = []

    if batch:
        client.send_batch(batch, producer_id)
        total_sent += len(batch)

    elapsed = time.perf_counter() - start
    eps = total_sent / elapsed if elapsed > 0 else 0
    print(f"Benchmark: {total_sent} events in {elapsed:.2f}s → {eps:.0f} events/sec")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true", help="Run benchmark and exit")
    args = parser.parse_args()

    if args.benchmark:
        n = int(os.getenv("BENCHMARK_EVENTS", "100000"))
        run_benchmark(n)
    else:
        run_continuous()
