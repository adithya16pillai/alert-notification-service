"""Snapshot (de)serialisation + the two-query build contract (03 §7, §9).

``test_build_snapshot_uses_at_most_two_queries`` is the runnable guard for the
"no N+1" acceptance criterion: regardless of how many subscriptions or channels
a tenant has, building the routing snapshot issues one query for subscriptions
and at most one for the channels they reference.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from app.recipients.cache import build_snapshot
from app.recipients.snapshot import SnapChannel, SnapSubscription, TenantSnapshot


def test_snapshot_json_round_trip():
    rid = uuid4()
    cid = str(uuid4())
    snap = TenantSnapshot(
        subscriptions=[SnapSubscription("auth.*", "high", [cid])],
        channels={cid: SnapChannel(rid, "webhook", "https://x/y", {"secret": "secret://ref"})},
    )
    restored = TenantSnapshot.from_json(snap.to_json())
    assert restored.subscriptions == snap.subscriptions
    assert restored.channels == snap.channels
    assert restored.channels[cid].recipient_id == rid  # UUID survives the round trip


# --- fake session that records how many queries build_snapshot issues -------- #


class _Scalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _Scalars(self._rows)


class _FakeSession:
    """Returns queued result sets in order and counts ``execute`` calls."""

    def __init__(self, result_sets):
        self._queue = list(result_sets)
        self.calls = 0

    async def execute(self, _stmt):
        self.calls += 1
        return _Result(self._queue.pop(0) if self._queue else [])


class _Sub:
    def __init__(self, channel_ids):
        self.topic_pattern = "auth.*"
        self.min_severity = "info"
        self.channel_ids = channel_ids


class _Chan:
    def __init__(self, cid, rid):
        self.id = cid
        self.recipient_id = rid
        self.kind = "email"
        self.address = "a@x.com"
        self.config = {}


async def test_build_snapshot_uses_at_most_two_queries():
    rid = uuid4()
    cids = [uuid4() for _ in range(5)]
    subs = [_Sub([str(c) for c in cids[:3]]), _Sub([str(c) for c in cids[3:]])]
    channels = [_Chan(c, rid) for c in cids]

    session = _FakeSession([subs, channels])
    snap = await build_snapshot(session, "tenant-1")

    assert session.calls == 2  # subscriptions, then channels-by-id — never N+1
    assert len(snap.subscriptions) == 2
    assert len(snap.channels) == 5


async def test_build_snapshot_skips_channel_query_when_no_channel_ids():
    # No subscriptions reference any channel -> only the subscriptions query runs.
    session = _FakeSession([[_Sub([])]])
    snap = await build_snapshot(session, "tenant-1")

    assert session.calls == 1
    assert snap.channels == {}


@pytest.mark.asyncio
async def test_build_snapshot_empty_tenant_one_query():
    session = _FakeSession([[]])
    snap = await build_snapshot(session, "tenant-1")
    assert session.calls == 1
    assert snap.subscriptions == []
    assert snap.channels == {}
