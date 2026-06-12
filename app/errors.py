"""Single error hierarchy for the whole service.

Every error carries a stable ``code`` (for clients / dashboards) and an
``http_status``. The API layer renders them into the service-wide error schema
(01 §5): ``{"error": {"code", "message", "field", "trace_id"}}``.
"""

from __future__ import annotations


class AppError(Exception):
    """Base class for all application errors."""

    code: str = "internal_error"
    http_status: int = 500

    def __init__(self, message: str | None = None, *, field: str | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.field = field

    def to_dict(self, trace_id: str) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "field": self.field,
                "trace_id": trace_id,
            }
        }


# --- 4xx: caller's fault ---
class ValidationError(AppError):
    code = "validation_error"
    http_status = 400


class AuthError(AppError):
    code = "unauthorized"
    http_status = 401


class ForbiddenError(AppError):
    code = "forbidden"
    http_status = 403


class NotFoundError(AppError):
    code = "not_found"
    http_status = 404


class IdempotencyConflict(AppError):
    """Same Idempotency-Key seen with a different payload."""

    code = "idempotency_conflict"
    http_status = 409


class PayloadTooLargeError(AppError):
    code = "payload_too_large"
    http_status = 413


class RateLimitedError(AppError):
    code = "rate_limited"
    http_status = 429


# --- 5xx / dependency failures ---
class ServiceUnavailableError(AppError):
    code = "service_unavailable"
    http_status = 503


class QueueError(ServiceUnavailableError):
    code = "queue_error"


class ChannelError(AppError):
    """Base for channel-adapter delivery failures."""

    code = "channel_error"
    http_status = 502


class ProviderTimeout(ChannelError):
    code = "provider_timeout"
    http_status = 504


class CircuitOpenError(ChannelError):
    code = "circuit_open"
    http_status = 503
