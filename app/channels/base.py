"""Channel adapter contract + a reusable async circuit breaker.

Every adapter implements ``Channel.send`` behind one interface (04 §6); the
dispatcher only ever talks to this interface, so a new channel is a new subclass
and a registry line — nothing else changes (04 §10 "≤ 1 day to add a channel").

Three things live here:
  - ``DeliveryRequest`` / ``DeliveryResult`` — the wire types. Results carry the
    three-way classification from 04 §6 (``sent`` / ``transient`` / ``permanent``).
  - ``CircuitBreaker`` — per-channel, opens after N consecutive failures (04 §4).
  - ``Channel`` / ``HttpChannel`` — base classes. ``HttpChannel`` owns one shared
    ``httpx.AsyncClient`` per adapter (not per request), with HTTP/2, pooling, and
    the channel's policy timeout (04 §9).
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from uuid import UUID

import httpx

from app.channels.classification import Outcome
from app.channels.policy import ChannelPolicy, get_policy
from app.config import get_settings
from app.errors import CircuitOpenError
from app.observability.metrics import circuit_breaker_state


@dataclass(frozen=True)
class DeliveryRequest:
    alert_id: str  # ULID
    recipient_id: UUID
    target: str
    title: str
    body: str
    severity: str
    config: dict = field(default_factory=dict)
    tenant: str = ""
    # Pre-rendered message (04 §8). Falls back to title/body when empty so an
    # adapter is usable without the renderer (e.g. in isolation tests).
    rendered_subject: str = ""
    rendered_body: str = ""

    @property
    def subject(self) -> str:
        return self.rendered_subject or self.title

    @property
    def message(self) -> str:
        return self.rendered_body or self.body


@dataclass(frozen=True)
class DeliveryResult:
    """Outcome of one provider call. Construct via the classmethods so the
    status and the derived ``ok``/``retryable`` flags can never disagree."""

    status: Outcome
    provider_id: str | None = None
    error: str | None = None
    # Seconds the provider asked us to wait (429 ``Retry-After``); the dispatcher
    # prefers this over computed backoff when present (04 §9, Slack/webhook).
    retry_after_s: float | None = None

    @property
    def ok(self) -> bool:
        return self.status is Outcome.SENT

    @property
    def retryable(self) -> bool:
        return self.status is Outcome.TRANSIENT_FAILURE

    @classmethod
    def sent(cls, provider_id: str | None = None) -> DeliveryResult:
        return cls(status=Outcome.SENT, provider_id=provider_id)

    @classmethod
    def transient(cls, error: str, *, retry_after_s: float | None = None) -> DeliveryResult:
        return cls(status=Outcome.TRANSIENT_FAILURE, error=error, retry_after_s=retry_after_s)

    @classmethod
    def permanent(cls, error: str) -> DeliveryResult:
        return cls(status=Outcome.PERMANENT_FAILURE, error=error)


class CircuitBreaker:
    """Per-channel breaker. Opens after N consecutive failures, half-opens after
    a cooldown. Clock is ``time.monotonic`` (no wall clock, immune to NTP steps).

    A per-*provider* breaker is the whole point: Slack going down must not stop
    email. A global breaker would couple unrelated failures (04 §9, §11).
    """

    def __init__(self, name: str = "") -> None:
        s = get_settings()
        self._name = name
        self._threshold = s.circuit_failure_threshold
        self._reset_after = s.circuit_reset_seconds
        self._failures = 0
        self._opened_at: float | None = None

    def _set_state_metric(self, value: int) -> None:
        if self._name:
            circuit_breaker_state.labels(channel=self._name).set(value)

    def check(self) -> None:
        if self._opened_at is None:
            return
        if time.monotonic() - self._opened_at >= self._reset_after:
            self._opened_at = None  # half-open: allow one trial
            self._set_state_metric(2)  # half-open
            return
        raise CircuitOpenError("channel circuit open")

    def record(self, ok: bool) -> None:
        if ok:
            self._failures = 0
            self._opened_at = None
            self._set_state_metric(0)  # closed
        else:
            self._failures += 1
            if self._failures >= self._threshold:
                self._opened_at = time.monotonic()
                self._set_state_metric(1)  # open


class Channel(ABC):
    """Base adapter. Subclasses set ``name`` and implement ``_deliver``.

    ``policy`` (timeout, retry budget, backoff) comes from the 04 §5 table keyed
    by ``name``; the dispatcher reads it to drive retries.
    """

    name: str

    def __init__(self) -> None:
        self._breaker = CircuitBreaker(self.name)
        self.policy: ChannelPolicy = get_policy(self.name)

    async def send(self, req: DeliveryRequest) -> DeliveryResult:
        self._breaker.check()
        result = await self._deliver(req)
        self._breaker.record(result.ok)
        return result

    @abstractmethod
    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        """Perform the actual provider call. Must honour the channel timeout and
        never raise for ordinary provider failures — return a classified result."""
        raise NotImplementedError

    async def health_check(self) -> bool:
        """Cheap liveness probe (04 §6). Default: breaker not open."""
        try:
            self._breaker.check()
            return True
        except CircuitOpenError:
            return False

    async def aclose(self) -> None:  # noqa: B027 - intentional no-op default hook
        """Release adapter resources. Overridden by HTTP adapters; a no-op for
        adapters (e.g. SMTP email) that hold no long-lived client."""
        return


class HttpChannel(Channel):
    """Base for HTTP-transport adapters. Owns ONE shared ``httpx.AsyncClient``
    (connection pool + HTTP/2) for the adapter's lifetime — never per request,
    which would defeat pooling and blow the latency budget (04 §9)."""

    def __init__(self) -> None:
        super().__init__()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.policy.timeout_s),
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )

    @property
    def client(self) -> httpx.AsyncClient:
        return self._client

    async def aclose(self) -> None:
        await self._client.aclose()


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds form). HTTP-date form is
    rare for rate-limit responses; we ignore it and fall back to backoff."""

    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
