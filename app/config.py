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

    # --- Content dedup (06) ---
    # Two alerts about the same event within the window collapse to one dispatch.
    # All three are overridable per (tenant, topic) via the dedup_policies table.
    dedup_window_seconds: int = 300  # default dedup window (5 min)
    dedup_default_fields: list[str] = Field(default_factory=lambda: ["host", "region"])
    # §4 vs §7 of the PRD disagree on the default; we follow §4's explicit
    # reasoning — duplicate *critical* pages cause the most harm, so we dedupe
    # critical by default (bypass OFF). Tenants who want "page me every time"
    # set critical_bypass=True on their policy.
    dedup_critical_bypass: bool = False
    dedup_key_prefix: str = "dedup"
    dedup_fingerprint_version: int = 1  # bump when compute_fingerprint changes
    dedup_policy_cache_key_prefix: str = "dedup:policy:tenant"
    dedup_policy_cache_ttl_seconds: int = 60

    # --- Rate limit (02) ---
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

    # --- Recipients / subscriptions (03) ---
    # Per-tenant routing snapshot cache. The TTL is the upper bound on staleness
    # if pub/sub invalidation fails entirely (03 §7).
    subs_cache_key_prefix: str = "subs:tenant"
    subs_cache_ttl_seconds: int = 60
    subs_invalidate_channel: str = "cache:subs:invalidate"
    # Cursor pagination: default page 50, hard cap 200 -> 400 (03 §4).
    list_default_limit: int = 50
    list_max_limit: int = 200

    # --- Channel adapters (04) ---
    # Per-channel timeout/retry/backoff are channel constants (see
    # app/channels/policy.py, 04 §5). These remain as conservative fallbacks /
    # circuit-breaker tunables shared across channels.
    channel_timeout_seconds: float = 5.0
    channel_max_retries: int = 3
    circuit_failure_threshold: int = 5
    circuit_reset_seconds: int = 30
    # Provider credentials (04 §9): resolved from a secrets backend at runtime.
    # "env" reads ANS_SECRET_<NAME> (local/dev); "aws" reads AWS Secrets Manager.
    secrets_backend: str = "env"
    aws_secrets_prefix: str = "ans/"
    # Email via SES SMTP (04 §9). STARTTLS is required; a downgrade is rejected.
    # Credentials (user/password) come from the secrets backend, not these vars.
    smtp_host: str = "email-smtp.us-east-1.amazonaws.com"
    smtp_port: int = 587
    smtp_from: str = "alerts@example.com"
    smtp_user_secret: str = "ses_smtp_user"
    smtp_password_secret: str = "ses_smtp_password"
    # Webhook HMAC signing (04 §9): sign the body so receivers can verify it.
    webhook_signing_enabled: bool = True
    # Slack bot token secret name (resolved via the secrets backend).
    slack_token_secret: str = "slack_bot_token"
    # Twilio (SMS) credential secret names + sender id.
    twilio_sid_secret: str = "twilio_account_sid"
    twilio_token_secret: str = "twilio_auth_token"
    twilio_from_number: str = ""

    # --- Auth (08) ---
    api_keys: list[str] = Field(default_factory=list)

    # --- Observability (09) ---
    otel_exporter_endpoint: str | None = None
    metrics_enabled: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
