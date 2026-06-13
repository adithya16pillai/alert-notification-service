"""Centralised configuration via Pydantic Settings.

All tunables live here so the service's signal-to-noise behaviour (dedup window,
rate-limit policy, queue batch sizes) is configuration, not code.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ANS_",
        extra="ignore",
    )

    # --- App ---
    app_name: str = "alert-notification-service"
    env: str = "local"
    debug: bool = False
    log_level: str = "INFO"

    # --- Datastores ---
    database_url: str = "postgresql+asyncpg://ans:ans@localhost:5432/ans"
    redis_url: str = "redis://localhost:6379/0"

    # --- Ingestion (01) ---
    ingest_max_body_bytes: int = 256 * 1024  # hard cap; larger => 413 (01 §7)
    idempotency_ttl_seconds: int = 24 * 60 * 60  # 24h idempotency window

    # --- Dedup / rate limit (02) ---
    dedup_window_seconds: int = 300
    rate_limit_capacity: int = 20  # token-bucket size per recipient+channel
    rate_limit_refill_per_sec: float = 1.0

    # --- Queue / dispatcher (02 / 05) ---
    queue_key_prefix: str = "queue:alerts"
    worker_batch_size: int = 50
    worker_poll_interval_ms: int = 100
    severities: tuple[str, ...] = ("critical", "high", "medium", "low", "info")
    # Starvation guard (02 §3): every Nth pop drains lowest-severity-first so a
    # sustained high-severity flood can never park a non-empty lower queue forever.
    queue_starvation_factor: int = 10
    starvation_counter_key: str = "queue:alerts:starvation_counter"
    # Visibility timeout (02 §6): popped-but-unacked alerts sit in this ZSET keyed
    # by deadline; the reaper re-queues any whose deadline passed (worker died).
    inflight_key: str = "queue:alerts:inflight"
    inflight_ttl_seconds: int = 60
    # Backpressure (02 §6): shed `info` ingestion with 503 once its backlog is huge.
    # Off by default (opt-in per-tenant policy in v2); critical is never shed.
    info_shed_enabled: bool = False
    info_shed_threshold: int = 100_000

    # --- Channel adapters (04) ---
    channel_timeout_seconds: float = 5.0
    channel_max_retries: int = 3
    circuit_failure_threshold: int = 5
    circuit_reset_seconds: int = 30

    # --- Auth (08) ---
    api_keys: list[str] = Field(default_factory=list)

    # --- Observability (09) ---
    otel_exporter_endpoint: str | None = None
    metrics_enabled: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
