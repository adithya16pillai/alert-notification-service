"""DLQ stream behaviour (07 §5 acceptance criteria) against fakeredis.

Push enriches the entry, list paginates newest-first with a cursor, get/delete
are id-addressed, and replay re-parks the delivery and removes the entry. Replay's
Postgres write (the fresh attempt row) is stubbed; we assert the Redis-visible
effects."""

from __future__ import annotations

import pytest

import app.dlq.service as svc
from app.config import get_settings
from app.dlq.service import (
    delete_entry,
    get_entry,
    list_entries,
    push,
    replay,
)
from app.queue.retry_queue import DeferredDelivery

pytestmark = pytest.mark.usefixtures("fake_redis")


async def _push(reason="exhausted_retries", channel="slack", **over):
    kw = dict(
        alert_id="01ABCDEF",
        recipient_id="11111111-1111-1111-1111-111111111111",
        channel=channel,
        target="https://hooks.example/x",
        severity="high",
        tenant="acme",
        reason=reason,
        last_error="boom",
        attempt_history=[{"attempt": 1, "status": "transient_failure", "error": "boom"}],
        config={"signing_secret": "shh"},
    )
    kw.update(over)
    return await push(**kw)


async def test_push_enriches_entry_and_is_retrievable():
    sid = await _push()
    entry = await get_entry(sid)
    assert entry is not None
    assert entry.reason == "exhausted_retries"
    assert entry.channel == "slack"
    assert entry.attempt_history == [
        {"attempt": 1, "status": "transient_failure", "error": "boom"}
    ]
    # config carries the secret internally (needed for replay) but never via the API.
    assert entry.config == {"signing_secret": "shh"}


async def test_last_error_is_truncated(fake_redis):
    cap = get_settings().dlq_max_error_bytes
    sid = await _push(last_error="x" * (cap + 500))
    entry = await get_entry(sid)
    assert len(entry.last_error) == cap


async def test_list_is_newest_first_with_cursor_pagination():
    ids = [await _push(last_error=f"e{i}") for i in range(5)]

    page1, cursor = await list_entries(limit=2)
    assert [e.stream_id for e in page1] == [ids[4], ids[3]]  # newest first
    assert cursor is not None

    page2, cursor2 = await list_entries(cursor=cursor, limit=2)
    assert [e.stream_id for e in page2] == [ids[2], ids[1]]

    page3, cursor3 = await list_entries(cursor=cursor2, limit=2)
    assert [e.stream_id for e in page3] == [ids[0]]
    assert cursor3 is None  # no more pages


async def test_delete_removes_the_entry():
    sid = await _push()
    assert await delete_entry(sid) is True
    assert await get_entry(sid) is None
    assert await delete_entry(sid) is False  # idempotent


async def test_replay_reparks_delivery_and_removes_entry(monkeypatch, fake_redis):
    recorded = []

    async def fake_record(session, **kw):
        recorded.append(kw)

    monkeypatch.setattr(svc, "record_attempt", fake_record)

    sid = await _push(severity="critical", channel="webhook")
    entry = await replay(session=None, stream_id=sid, actor="oncall")

    assert entry.stream_id == sid
    # 1) a fresh attempt row was opened (attempt 1, pending)
    assert recorded and recorded[0]["status"] == "pending"
    # 2) the delivery is re-parked on the retry queue for its severity...
    members = await fake_redis.zrange(f"{get_settings().retry_queue_key_prefix}:critical", 0, -1)
    assert len(members) == 1
    parked = DeferredDelivery.from_member(members[0])
    assert parked.reason == "retry" and parked.attempt_no == 0
    # 3) ...and the DLQ entry is gone so it can't replay twice.
    assert await get_entry(sid) is None
