"""Channel adapter contract + a reusable async circuit breaker.

Every adapter implements ``Channel.send`` behind one interface (04 §6); the
dispatcher only ever talks to this interface, so a new channel is a new subclass
and a registry line — nothing else changes (04 §10 "≤ 1 day to add a channel").

Three things live here:
  - ``DeliveryRequest`` / ``DeliveryResult`` — the wire types. Results carry the
    three-way classification from 04 §6 (``sent`` / ``transient`` / ``permanent``).
  - ``Channel`` / ``HttpChannel`` — base classes wired to the per-provider
    Redis circuit breaker (07 §4). ``HttpChannel`` owns one shared
    ``httpx.AsyncClient`` per adapter (not per request), with HTTP/2, pooling, and
    the channel's policy timeout (04 §9).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from uuid import UUID

import httpx

from app.channels.circuit import get_breaker
from app.channels.classification import Outcome
from app.channels.policy import ChannelPolicy, get_policy
from app.observability.metrics import circuit_breaker_state

#: Breaker state string -> the gauge's numeric encoding (0=closed,1=open,2=half).
_STATE_GAUGE = {"closed": 0, "open": 1, "half_open": 2, "probe": 2}


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


class Channel(ABC):
    """Base adapter. Subclasses set ``name`` and implement ``_deliver``.

    ``policy`` (timeout, retry budget, backoff) comes from the 04 §5 table keyed
    by ``name``; the dispatcher reads it to drive retries. Every call is guarded
    by the per-provider Redis circuit breaker (07 §4).
    """

    name: str

    def __init__(self) -> None:
        self._breaker = get_breaker()
        self.policy: ChannelPolicy = get_policy(self.name)

    def provider_key(self, req: DeliveryRequest) -> str:
        """Breaker identity for this request. Single-endpoint channels share one
        provider (the channel name); webhooks override this to key per receiver so
        one broken receiver can't trip every webhook (07 §4.2)."""
        return self.name

    async def send(self, req: DeliveryRequest) -> DeliveryResult:
        """Guarded send: when the provider's breaker is open we fast-fail in
        <10ms with a *transient* result (07 §4.4) — never calling the provider —
        so the dispatcher reschedules it. The retry delay typically exceeds the
        open timeout, so the next attempt meets a half-open circuit."""
        provider = self.provider_key(req)
        if await self._breaker.allow(provider) == "open":
            circuit_breaker_state.labels(channel=self.name).set(_STATE_GAUGE["open"])
            return DeliveryResult.transient("circuit_open")
        result = await self._deliver(req)
        new_state = await self._breaker.record(provider, result.ok)
        circuit_breaker_state.labels(channel=self.name).set(_STATE_GAUGE[new_state])
        return result

    @abstractmethod
    async def _deliver(self, req: DeliveryRequest) -> DeliveryResult:
        """Perform the actual provider call. Must honour the channel timeout and
        never raise for ordinary provider failures — return a classified result."""
        raise NotImplementedError

    async def health_check(self) -> bool:
        """Cheap liveness probe (04 §6). Default: the provider's breaker is not
        open. Read-only — it does not consume the half-open probe."""
        return await self._breaker.state(self.provider_key_for_health()) != "open"

    def provider_key_for_health(self) -> str:
        """Provider identity for health checks. Defaults to the channel name;
        per-receiver channels (webhook) have no single health identity, so they
        report on the channel-level key."""
        return self.name

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
