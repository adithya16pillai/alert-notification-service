"""Database-level acceptance criteria for recipients/subscriptions (03 §9).

These need a real Postgres (UUID / JSONB / ARRAY column types and row-value
cursor comparison have no faithful SQLite equivalent), so the whole module skips
when ``ANS_DATABASE_URL`` isn't reachable — same philosophy as the fakeredis
gate on the queue tests. Under docker-compose (``ans:ans@postgres``) they run.

Covered:
  * cursor pagination is stable under a mid-listing insert (no skips/dupes),
  * soft-deleting a recipient removes it (and its channels) from matching,
  * cross-tenant access returns 404, never 403 (don't leak existence).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings
from app.db import Base
from app.errors import NotFoundError
from app.recipients import cache, service
from app.recipients.models import Channel, Recipient, Subscription
from app.recipients.schemas import ChannelIn, SubscriptionIn

pytestmark = pytest.mark.usefixtures("fake_redis")  # cache invalidation -> fakeredis

_TABLES = [Subscription.__table__, Channel.__table__, Recipient.__table__]


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE SCHEMA IF NOT EXISTS recipients"))
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
    except Exception as exc:  # noqa: BLE001 — any connect/auth error => skip
        await engine.dispose()
        pytest.skip(f"Postgres unavailable: {exc!r}")

    sm = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sm() as s:
            yield s
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all, tables=_TABLES)
        await engine.dispose()
        cache.clear_local()


async def _recipient(session, tenant, name, created_at):
    r = Recipient(tenant_id=tenant, name=name, created_at=created_at)
    session.add(r)
    await session.commit()
    await session.refresh(r)
    return r


async def test_cursor_pagination_is_stable_under_insert(session):
    base = datetime(2026, 6, 13, tzinfo=UTC)
    # Five recipients with strictly increasing created_at => deterministic order.
    made = [await _recipient(session, "t1", f"r{i}", base + timedelta(minutes=i)) for i in range(5)]
    newest_first = [r.id for r in reversed(made)]  # r4, r3, r2, r1, r0

    page1, cur1 = await service.list_recipients(session, tenant="t1", cursor=None, limit=2)
    assert [r.id for r in page1] == newest_first[:2]
    assert cur1 is not None

    # An insert *newer* than the cursor position happens mid-pagination...
    await _recipient(session, "t1", "r5", base + timedelta(minutes=99))

    page2, cur2 = await service.list_recipients(session, tenant="t1", cursor=cur1, limit=2)
    page3, cur3 = await service.list_recipients(session, tenant="t1", cursor=cur2, limit=2)

    seen = [r.id for r in page1 + page2 + page3]
    # ...the rows that existed when we started are returned exactly once each,
    # none skipped, none duplicated. r5 doesn't gatecrash the in-flight scan.
    assert seen == newest_first
    assert len(set(seen)) == len(seen)
    assert cur3 is None  # exhausted


async def test_soft_deleted_recipient_drops_out_of_matching(session):
    rcpt = await _recipient(session, "t1", "on-call", datetime(2026, 6, 13, tzinfo=UTC))
    channel = await service.add_channel(
        session, tenant="t1", recipient_id=rcpt.id, body=ChannelIn(kind="email", address="a@x.com")
    )
    await service.create_subscription(
        session,
        tenant="t1",
        body=SubscriptionIn(
            recipient_id=rcpt.id, topic_pattern="auth.*", channel_ids=[channel.id]
        ),
    )

    before = await service.resolve_targets(
        session, tenant="t1", topic="auth.login", severity="high"
    )
    assert [t.target for t in before] == ["a@x.com"]

    await service.delete_recipient(session, tenant="t1", recipient_id=rcpt.id)

    after = await service.resolve_targets(session, tenant="t1", topic="auth.login", severity="high")
    assert after == []  # recipient + its channel are soft-deleted -> no match
    # Cascade also soft-deleted the subscription row (history preserved).
    rows = (await session.execute(Subscription.__table__.select())).all()
    assert all(row.deleted_at is not None for row in rows)


async def test_cross_tenant_access_returns_404_not_403(session):
    rcpt = await _recipient(session, "tenant-a", "secret", datetime(2026, 6, 13, tzinfo=UTC))

    # Tenant B asking for tenant A's id must look identical to "doesn't exist".
    with pytest.raises(NotFoundError):
        await service.get_recipient(session, tenant="tenant-b", recipient_id=rcpt.id)

    # And a subscription referencing another tenant's recipient is rejected.
    with pytest.raises(NotFoundError):
        await service.create_subscription(
            session,
            tenant="tenant-b",
            body=SubscriptionIn(
                recipient_id=rcpt.id, topic_pattern="*", channel_ids=[uuid4()]
            ),
        )
