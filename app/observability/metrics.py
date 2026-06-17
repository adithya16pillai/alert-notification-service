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
# Content dedup (06): suppressed duplicates. Per-tenant dedup rate is derived
# from the alerts table (status='deduped'); severity keeps the label cardinality
# bounded here.
alerts_deduped_total = Counter(
    "ans_alerts_deduped_total", "Alerts suppressed as content duplicates", ["severity"]
)
queue_depth = Gauge("ans_queue_depth", "Items in priority queue", ["severity"])
dlq_depth = Gauge("ans_dlq_depth", "Items in dead-letter queue")
# DLQ accounting (07 §5): terminal failures pushed in, by entry reason; and
# operator-initiated replays back onto the delivery path.
dlq_pushed_total = Counter(
    "ans_dlq_pushed_total",
    "Delivery attempts abandoned to the DLQ",
    ["channel", "reason"],  # exhausted_retries | permanent_failure | rate_limit_expired
)
dlq_replays_total = Counter("ans_dlq_replays_total", "DLQ entries replayed by an operator")
# Priority queue (02): critical time-to-dispatch + in-flight/visibility health.
critical_ttd_seconds = Histogram(
    "ans_critical_ttd_seconds",
    "Critical alert time from receipt to worker pickup (target p99 < 1s, 02 §4)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
)
inflight_depth = Gauge("ans_inflight_depth", "Alerts popped but not yet acked")
inflight_reaped_total = Counter(
    "ans_inflight_reaped_total", "In-flight alerts re-queued after visibility timeout"
)
backpressure_shed_total = Counter(
    "ans_backpressure_shed_total", "Alerts shed at ingest by backpressure", ["severity"]
)
# Subscription cache (03): hit-rate drives end-to-end dispatch latency.
subscription_cache_ops_total = Counter(
    "ans_subscription_cache_ops_total",
    "Subscription snapshot lookups",
    ["result"],  # hit_local | hit_redis | miss
)
subscription_cache_invalidations_total = Counter(
    "ans_subscription_cache_invalidations_total",
    "Subscription cache invalidations published on a recipients write",
)
rate_limit_denials_total = Counter(
    "ans_rate_limit_denials_total", "Deliveries denied by rate limit", ["channel"]
)
# Deferral-instead-of-drop accounting (05 §7). Recipient is deliberately NOT a
# label (unbounded cardinality); per-recipient denial alerting is done off logs.
rate_limit_deferred_total = Counter(
    "ans_rate_limit_deferred_total", "Deliveries deferred (parked for retry)", ["channel"]
)
rate_limit_abandoned_total = Counter(
    "ans_rate_limit_abandoned_total",
    "Deliveries abandoned to DLQ after exceeding the deferral cap",
    ["channel"],
)
retry_queue_depth = Gauge(
    "ans_retry_queue_depth", "Items parked in the deferred-retry queue", ["severity"]
)
delivery_attempts_total = Counter(
    "ans_delivery_attempts_total", "Delivery attempts", ["channel", "status"]
)
# Per-channel adapter health (04): error rate + circuit-breaker state panel.
delivery_errors_total = Counter(
    "ans_delivery_errors_total",
    "Delivery failures by channel and classification",
    ["channel", "classification"],  # transient_failure | permanent_failure
)
circuit_breaker_state = Gauge(
    "ans_circuit_breaker_state",
    "Per-channel circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["channel"],
)
delivery_latency = Histogram(
    "ans_delivery_latency_seconds", "End-to-end delivery latency", ["severity"]
)

metrics_router = APIRouter()


@metrics_router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
