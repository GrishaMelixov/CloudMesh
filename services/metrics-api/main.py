"""metrics-api entry point.

FastAPI application exposing analytics queries over ClickHouse.
Swagger UI available at http://localhost:8000/docs
"""

import logging
import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from routers import analytics, health

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI(
    title="CloudMesh Metrics API",
    description=(
        "Analytics API for the CloudMesh distributed log platform.\n\n"
        "Queries hit ClickHouse materialised views for fast responses "
        "over large datasets."
    ),
    version="1.0.0",
)

app.include_router(health.router)
app.include_router(analytics.router)


@app.get("/")
def root():
    return {
        "service": "metrics-api",
        "docs": "/docs",
        "endpoints": [
            "GET /health",
            "GET /health/ready",
            "GET /analytics/stats",
            "GET /analytics/services",
            "GET /analytics/error-rate",
            "GET /analytics/volume",
            "GET /analytics/logs",
        ],
    }


if __name__ == "__main__":
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
