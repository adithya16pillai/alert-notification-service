"""Prometheus metrics: four golden signals + product-specific gauges.

Product metrics (00 §2): queue depth, DLQ depth, rate-limit denials.
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# --- Golden signals ---
http_requests_total = Counter(
    "ans_http_requests_total", "HTTP requests", ["method", "path", "status"]
)
http_request_latency = Histogram(
    "ans_http_request_latency_seconds", "HTTP request latency", ["method", "path"]
)

# --- Product-specific ---
alerts_ingested_total = Counter("ans_alerts_ingested_total", "Alerts accepted", ["severity"])
queue_depth = Gauge("ans_queue_depth", "Items in priority queue", ["severity"])
dlq_depth = Gauge("ans_dlq_depth", "Items in dead-letter queue")
rate_limit_denials_total = Counter(
    "ans_rate_limit_denials_total", "Deliveries denied by rate limit", ["channel"]
)
delivery_attempts_total = Counter(
    "ans_delivery_attempts_total", "Delivery attempts", ["channel", "status"]
)
delivery_latency = Histogram(
    "ans_delivery_latency_seconds", "End-to-end delivery latency", ["severity"]
)

metrics_router = APIRouter()


@metrics_router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
