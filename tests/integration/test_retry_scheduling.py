"""Queue-based retry + DLQ entry conditions (07 §3.3, §5.1, §6 AC1).

Drives ``Dispatcher._attempt`` with a fake channel against fakeredis. The DB write
(``_record``) is captured in memory; rescheduling and DLQ pushes hit the fake
Redis. We pull each reschedule straight off the retry ZSET and feed it back in,
simulating the retry worker without the Lua/clock machinery.
"""

from __future__ import annotations

import pytest

import app.dispatcher.worker as worker
from app.channels.base import DeliveryResult
from app.channels.policy import ChannelPolicy
from app.config import get_settings
from app.dispatcher.worker import Dispatcher, _AlertSnapshot
from app.dlq.service import list_entries
from app.queue.retry_queue import DeferredDelivery
from app.recipients.schemas import ResolvedTarget

pytestmark = pytest.mark.usefixtures("fake_redis")

RID = "11111111-1111-1111-1111-111111111111"
MAX_RETRIES = 3


class _FakeChannel:
    def __init__(self, result: DeliveryResult) -> None:
        self.policy = ChannelPolicy(
            timeout_s=5, max_retries=MAX_RETRIES, backoff_base_s=0.0, backoff_cap_s=0.0
        )
        self._result = result
        self.calls = 0

    async def send(self, req):  # noqa: ANN001
        self.calls += 1
        return self._result


def _target():
    return ResolvedTarget(recipient_id=RID, channel="slack", target="x", config={})


def _snapshot():
    return _AlertSnapshot(id="01ABC", title="t", body="b", severity="high", tenant="t1")


def _wire(monkeypatch, result) -> tuple[Dispatcher, list, _FakeChannel]:
    ch = _FakeChannel(result)
    records: list = []

    monkeypatch.setattr(worker, "get_channel", lambda _kind: ch)

    d = Dispatcher()
    d._build_request = lambda alert, target: None  # type: ignore[method-assign,assignment]
    d._load_snapshot = lambda _aid: _ret(_snapshot())  # type: ignore[method-assign,assignment]

    async def fake_record(status, alert_id, target, retry_count, provider_id, error):
        records.append((status, retry_count))

    d._record = fake_record  # type: ignore[method-assign]
    return d, records, ch


def _ret(value):
    async def _coro(*_a, **_k):
        return value

    return _coro()


async def _drain_retry(fake_redis, severity="high") -> DeferredDelivery | None:
    key = f"{get_settings().retry_queue_key_prefix}:{severity}"
    members = await fake_redis.zrange(key, 0, -1)
    if not members:
        return None
    await fake_redis.zrem(key, members[0])
    return DeferredDelivery.from_member(members[0])


async def test_consistently_failing_channel_yields_max_retries_plus_one_rows_and_one_dlq(
    monkeypatch, fake_redis
):
    # AC1: a consistently-(transient)-failing channel => exactly max_retries+1
    # delivery_attempts rows and one DLQ entry.
    d, records, _ = _wire(monkeypatch, DeliveryResult.transient("5xx"))

    await d._attempt(_snapshot(), _target(), attempt_no=0, history=[])
    # Feed each scheduled retry back until the queue drains.
    while (delivery := await _drain_retry(fake_redis)) is not None:
        await d._process_retry(delivery)

    assert len(records) == MAX_RETRIES + 1  # 4 attempt rows
    statuses = [s for s, _ in records]
    assert statuses == ["failed", "failed", "failed", "abandoned"]

    entries, _ = await list_entries(limit=10)
    assert len(entries) == 1
    assert entries[0].reason == "exhausted_retries"
    assert len(entries[0].attempt_history) == MAX_RETRIES + 1


async def test_permanent_failure_dlqs_after_one_attempt(monkeypatch, fake_redis):
    # §5.1: a permanent failure goes straight to the DLQ — no retries.
    d, records, ch = _wire(monkeypatch, DeliveryResult.permanent("bad address"))

    await d._attempt(_snapshot(), _target(), attempt_no=0, history=[])

    assert ch.calls == 1
    assert records == [("abandoned", 1)]
    assert await _drain_retry(fake_redis) is None  # nothing rescheduled
    entries, _ = await list_entries(limit=10)
    assert len(entries) == 1
    assert entries[0].reason == "permanent_failure"


async def test_success_records_one_sent_row_and_no_dlq(monkeypatch, fake_redis):
    d, records, ch = _wire(monkeypatch, DeliveryResult.sent(provider_id="ok"))

    await d._attempt(_snapshot(), _target(), attempt_no=0, history=[])

    assert records == [("sent", 1)]
    entries, _ = await list_entries(limit=10)
    assert entries == []
