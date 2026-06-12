"""Channel adapter contract + a reusable async circuit breaker.

Every adapter implements ``Channel.send`` with timeout + retry + circuit
breaker (00 §7.4 step 9). The dispatcher only ever talks to this interface.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from uuid import UUID

from app.config import get_settings
from app.errors import CircuitOpenError


@dataclass(frozen=True)
class DeliveryRequest:
    alert_id: str  # ULID
    recipient_id: UUID
    target: str
    title: str
    body: str
    severity: str
    config: dict = field(default_factory=dict)


@dataclass(frozen=True)
class DeliveryResult:
    ok: bool
    provider_id: str | None = None
    error: str | None = None
    retryable: bool = False


class CircuitBreaker:
    """Per-channel breaker. Opens after N consecutive failures, half-opens
    after a cooldown. Clock is injected via ``time.monotonic`` (no wall clock).
    """

    def __init__(self) -> None:
        s = get_settings()
        self._threshold = s.circuit_failure_threshold
        self._reset_after = s.circuit_reset_seconds
        self._failures = 0
        self._opened_at: float | None = None

    def check(self) -> None:
        if self._opened_at is None:
            return
        if time.monotonic() - self._opened_at >= self._reset_after:
            self._opened_at = None  # half-open: allow one trial
            return
        raise CircuitOpenError("channel circuit open")

    def record(self, ok: bool) -> None:
        if ok:
            self._failures = 0
            self._opened_at = None
        else:
            self._failures += 1
            if self._failures >= self._threshold:
                self._opened_at = time.monotonic()


class Channel(ABC):
    """Base adapter. Subclasses set ``name`` and implement ``_deliver``."""

    name: str

    def __init__(self) -> None:
        self._breaker = CircuitBreaker()

    async def send(self, req: DeliveryRequest) -> DeliveryResult:
        self._breaker.check()
        result = await self._deliver(req)
        self._breaker.record(result.ok)
        return result

    @abstractmethod
    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        """Perform the actual provider call. Must honour the channel timeout."""
        raise NotImplementedError
