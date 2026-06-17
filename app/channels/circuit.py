"""Redis-backed, per-provider circuit breaker (07 §4).

The breaker state lives in Redis (``circuit:{provider}``), not in process memory,
because workers are stateless and horizontally scaled: any pod must see the same
open/closed decision, with no sticky routing (07 §4.3, §7). An in-process breaker
would let each pod independently hammer a downed provider.

It is **per-provider, not global** (07 §4.2) — the single most important property
here. A Slack outage opens only the Slack breaker; SES/Twilio/webhook keep
flowing. The ``provider`` key is the channel kind for single-endpoint channels
and ``webhook:{host}`` for webhooks, so one broken receiver can't trip the rest.

State machine (07 §4.1)::

    CLOSED --(threshold failures within window)--> OPEN
    OPEN   --(open_timeout elapsed)-------------->  HALF_OPEN (one probe)
    HALF_OPEN --(probe ok)----------------------->  CLOSED
    HALF_OPEN --(probe fails)--------------------->  OPEN

Both transitions are done inside Lua so the read-decide-write is atomic across
pods (no lost update when two workers race on the same provider). ``now`` is
passed in by the caller — never read from the server clock — mirroring the queue
scripts, which also makes the FSM deterministically testable.

The key carries a TTL slightly longer than the open timeout: if the single
half-open probe never reports back (its worker died), the key expires and the
breaker self-heals to CLOSED rather than wedging shut forever (07 §4.3).
"""

from __future__ import annotations

import time

from redis.commands.core import AsyncScript

from app.config import get_settings
from app.redis_client import get_redis

# --- allow(): decide whether a call may proceed, transitioning OPEN->HALF_OPEN
# and handing the *one* probe to this caller. Returns "closed" | "probe" | "open".
_ALLOW_LUA = """
local now          = tonumber(ARGV[1])
local open_timeout = tonumber(ARGV[2])
local ttl          = tonumber(ARGV[3])

local state     = redis.call('HGET', KEYS[1], 'state') or 'closed'
local opened_at = tonumber(redis.call('HGET', KEYS[1], 'opened_at')) or 0

if state == 'open' then
  if now - opened_at >= open_timeout then
    -- cooldown elapsed: move to half-open and grant THIS caller the single probe
    redis.call('HSET', KEYS[1], 'state', 'half_open', 'opened_at', now)
    redis.call('PEXPIRE', KEYS[1], ttl)
    return 'probe'
  end
  return 'open'
elseif state == 'half_open' then
  -- a probe is already in flight; everyone else fast-fails. If the probe is
  -- stale (its worker likely died) we hand out a fresh one instead of wedging.
  if now - opened_at >= open_timeout then
    redis.call('HSET', KEYS[1], 'opened_at', now)
    redis.call('PEXPIRE', KEYS[1], ttl)
    return 'probe'
  end
  return 'open'
end
return 'closed'
"""

# --- record(): fold one call outcome into the state. Returns the new state.
_RECORD_LUA = """
local ok        = ARGV[1] == '1'
local now       = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local window    = tonumber(ARGV[4])
local ttl       = tonumber(ARGV[5])

if ok then
  -- any success closes the circuit and clears the failure window
  redis.call('DEL', KEYS[1])
  return 'closed'
end

local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
if state == 'half_open' then
  -- the probe failed -> straight back to open, restart the cooldown
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', now)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return 'open'
end

-- closed: count failures inside a rolling window; trip once we hit threshold
local fails        = tonumber(redis.call('HGET', KEYS[1], 'fails')) or 0
local window_start = tonumber(redis.call('HGET', KEYS[1], 'window_start')) or 0
if now - window_start >= window then
  fails = 0
  window_start = now
end
fails = fails + 1
if fails >= threshold then
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', now,
             'fails', fails, 'window_start', window_start)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return 'open'
end
redis.call('HSET', KEYS[1], 'state', 'closed', 'fails', fails, 'window_start', window_start)
redis.call('PEXPIRE', KEYS[1], window)
return 'closed'
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


class RedisCircuitBreaker:
    """One instance serves every provider; the provider identity is the Redis key,
    so a single stateless object operates breakers for all channels/receivers."""

    def __init__(self) -> None:
        s = get_settings()
        self._prefix = s.circuit_key_prefix
        self._threshold = s.circuit_failure_threshold
        self._window_ms = s.circuit_failure_window_seconds * 1000
        self._open_timeout_ms = s.circuit_open_timeout_seconds * 1000
        self._ttl_ms = s.circuit_state_ttl_seconds * 1000
        self._allow_script: AsyncScript | None = None
        self._record_script: AsyncScript | None = None

    def _key(self, provider: str) -> str:
        return f"{self._prefix}:{provider}"

    def _scripts(self) -> tuple[AsyncScript, AsyncScript]:
        redis = get_redis()
        if self._allow_script is None:
            self._allow_script = redis.register_script(_ALLOW_LUA)
            self._record_script = redis.register_script(_RECORD_LUA)
        assert self._record_script is not None
        return self._allow_script, self._record_script

    async def allow(self, provider: str, *, now_ms: int | None = None) -> str:
        """Return ``"closed"`` (proceed), ``"probe"`` (proceed as the lone
        half-open trial), or ``"open"`` (fast-fail, do not call the provider)."""
        allow_script, _ = self._scripts()
        return await allow_script(
            keys=[self._key(provider)],
            args=[now_ms if now_ms is not None else _now_ms(), self._open_timeout_ms, self._ttl_ms],
        )

    async def record(self, provider: str, ok: bool, *, now_ms: int | None = None) -> str:
        """Fold one outcome into the breaker; return the resulting state."""
        _, record_script = self._scripts()
        return await record_script(
            keys=[self._key(provider)],
            args=[
                "1" if ok else "0",
                now_ms if now_ms is not None else _now_ms(),
                self._threshold,
                self._window_ms,
                self._ttl_ms,
            ],
        )

    async def state(self, provider: str, *, now_ms: int | None = None) -> str:
        """Read-only view of the current state (no probe consumed) — for health
        checks. Reflects the OPEN->HALF_OPEN cooldown without mutating anything."""
        h = await get_redis().hgetall(self._key(provider))
        if not h:
            return "closed"
        st = h.get("state", "closed")
        if st == "open":
            now = now_ms if now_ms is not None else _now_ms()
            if now - int(h.get("opened_at", 0)) >= self._open_timeout_ms:
                return "half_open"
        return st


#: One shared breaker for the whole process (state is in Redis, so sharing the
#: object is purely an optimisation — it holds no per-provider state).
_breaker = RedisCircuitBreaker()


def get_breaker() -> RedisCircuitBreaker:
    return _breaker
