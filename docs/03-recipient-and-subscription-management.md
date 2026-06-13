# 03 — Recipient & Subscription Management

> **Prerequisite reading:** [00-overview-and-architecture.md](./00-overview-and-architecture.md)
>
> **Status:** Implemented. Code: `app/recipients/` (`models.py`, `schemas.py`,
> `service.py`, `router.py`, `pagination.py`, `matching.py`, `snapshot.py`,
> `cache.py`), `app/auth/dependencies.py` (`require_tenant`). Tests:
> `tests/unit/test_recipient_pagination.py`, `tests/unit/test_subscription_matching.py`,
> `tests/unit/test_subscription_snapshot.py`, `tests/integration/test_subscription_cache.py`,
> `tests/integration/test_recipients_db.py` (Postgres-gated),
> `tests/contract/test_recipients_contract.py`.

---

## 1. Problem

We need to know who gets which alerts, on which channels, with what overrides. This is the routing graph between an incoming alert and the humans/services that should be notified about it.

---

## 2. Impact & Deliverables

### Why this matters
This is the self-service surface that lets customer admins manage their notification policy without filing tickets. Two failure modes here:

- **Manual support burden**: every team add, every channel change, every escalation rule becomes a support ticket. Linear cost growth that kills margin.
- **Cross-tenant leakage**: if Tenant A can see or modify Tenant B's recipients, we lose every enterprise contract. Multi-tenant isolation is a compliance requirement, not a nice-to-have.

Subscription matching is also a hot path inside the dispatcher — it runs on every alert. The cache strategy here directly affects end-to-end latency for the whole product.

### Deliverables to ship
- **Customer-facing**: admin REST API + OpenAPI docs, RBAC matrix (who can edit what), subscription pattern reference (glob syntax, examples).
- **Internal**: tenant-isolation test report (every endpoint exercised cross-tenant), subscription-cache-hit-rate dashboard.
- **Compliance**: data-access audit log spec (who changed which subscription, when).

### Success metrics
- Subscription change → live across all workers in < 5s (cache invalidation working).
- Zero cross-tenant reads or writes possible in security testing (RLS + app-layer scoping both pass).
- N+1 query absence: loading a single alert's matching recipients with all channels uses ≤ 2 queries (verified by `EXPLAIN`).

---

## 3. Functional requirements

- CRUD for recipients, channels (per recipient), and subscriptions (rules that match alerts to recipients).
- Subscriptions match on `tenant_id` + `topic` (glob-allowed, e.g. `auth.*`) + minimum severity.
- Each subscription specifies a list of channel IDs to route to.
- Soft delete: deleting a recipient marks it deleted, doesn't drop history.
- All list endpoints paginated by cursor (no `OFFSET`).

---

## 4. Non-functional requirements

- Subscription cache invalidation propagates to all worker processes within 1s of a write.
- Subscription matching on the dispatcher hot path uses no synchronous DB calls under normal conditions (cache-hit).
- All list endpoints reject `limit > 200` with `400`; default limit = 50.

---

## 5. Data model

```sql
CREATE SCHEMA recipients;

CREATE TABLE recipients.recipients (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    timezone    TEXT NOT NULL DEFAULT 'UTC',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at  TIMESTAMPTZ
);
CREATE INDEX idx_recipients_tenant ON recipients.recipients (tenant_id) WHERE deleted_at IS NULL;

CREATE TABLE recipients.channels (
    id            TEXT PRIMARY KEY,
    recipient_id  TEXT NOT NULL REFERENCES recipients.recipients(id),
    kind          TEXT NOT NULL CHECK (kind IN ('email','slack','webhook','sms')),
    address       TEXT NOT NULL,                       -- email addr, slack channel id, URL, phone
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    config        JSONB NOT NULL DEFAULT '{}'::jsonb,  -- secret refs, not raw secrets
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ
);
CREATE INDEX idx_channels_recipient ON recipients.channels (recipient_id) WHERE deleted_at IS NULL;

CREATE TABLE recipients.subscriptions (
    id                TEXT PRIMARY KEY,
    recipient_id      TEXT NOT NULL REFERENCES recipients.recipients(id),
    tenant_id         TEXT NOT NULL,
    topic_pattern     TEXT NOT NULL,                   -- glob, e.g. 'auth.*'
    min_severity      TEXT NOT NULL,
    channel_ids       TEXT[] NOT NULL,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at        TIMESTAMPTZ
);
CREATE INDEX idx_sub_tenant ON recipients.subscriptions (tenant_id) WHERE deleted_at IS NULL AND enabled = TRUE;
```

RLS policies (see [08-auth-multitenancy.md](./08-auth-multitenancy.md)) enforce tenant scoping at the DB layer.

---

## 6. API surface

All endpoints paginated by cursor:

```
GET    /v1/recipients?cursor=...&limit=50
POST   /v1/recipients
GET    /v1/recipients/{id}
PATCH  /v1/recipients/{id}
DELETE /v1/recipients/{id}                  (soft delete)

GET    /v1/recipients/{id}/channels
POST   /v1/recipients/{id}/channels
DELETE /v1/recipients/{id}/channels/{cid}

GET    /v1/subscriptions?cursor=...&limit=50
POST   /v1/subscriptions
PATCH  /v1/subscriptions/{id}
DELETE /v1/subscriptions/{id}
```

### Cursor format

```
cursor = base64( "{created_at_iso}|{id}" )
```

The query becomes: `WHERE (created_at, id) < (cursor.created_at, cursor.id) ORDER BY created_at DESC, id DESC LIMIT 50`. Stable under inserts, O(log n) on the index, never returns duplicates across pages.

### Response envelope

```json
{
  "items": [ ... ],
  "next_cursor": "eyJjcmVhdGVkX2F0Ijoi..." | null
}
```

`next_cursor: null` means no more pages.

---

## 7. Subscription matching (dispatcher hot path)

For each incoming alert:

1. Compute key: `(alert.tenant_id, alert.topic)`.
2. Look up `subs:tenant:{tenant_id}` in Redis (cached list of subscriptions for the tenant, TTL 60s).
3. Filter cached subscriptions in Python: `fnmatch(alert.topic, sub.topic_pattern)` AND `severity_rank(alert.severity) <= severity_rank(sub.min_severity)`.
4. Collect channel IDs from matching subscriptions, dedupe, hand off to dispatch.

Glob matching is fast enough at our scale; no need for materialised cross-products or trie structures.

### Cache invalidation

- Every write to `recipients.*` schema publishes to Redis pub/sub channel `cache:subs:invalidate` with the `tenant_id`.
- Worker processes subscribe to this channel and drop their local in-memory cache for that tenant (which is itself a wrapper over the Redis-cached subscription list).
- The 60s TTL is the upper bound for cache staleness even if pub/sub fails entirely.

---

## 8. Implementation notes

- **Secret refs in `channels.config`**: store strings like `secret://aws-sm/notif/webhook-acme`. Resolved at send time, never logged. See [09-cross-cutting-concerns.md](./09-cross-cutting-concerns.md) §4.
- **Channel verification**: `verified` defaults to false. A verification workflow (out of scope for v1 PRD, in scope for v2) sends a test alert and waits for the recipient to confirm.
- **Cascade on recipient delete**: subscriptions and channels are soft-deleted via a trigger or service-level cascade — keeps audit trail intact.
- **Unique constraint trap**: be careful with uniqueness on soft-deleted tables. Use partial unique indexes: `CREATE UNIQUE INDEX ... WHERE deleted_at IS NULL`.

---

## 9. Acceptance criteria

- [x] All list endpoints reject `limit > 200` with `400`, default to 50.
- [x] Cursor pagination is stable: inserting a new row mid-page does not cause skipped or duplicated rows.
- [x] Soft-deleted recipients do not match new alerts.
- [x] Subscription cache invalidates within 1s of an update across all worker processes (verified with 3-worker test).
- [x] No N+1: loading 1 alert's subscriptions including channels uses ≤ 2 queries (verified by `EXPLAIN` and instrumented test).
- [x] Cross-tenant test: a JWT for tenant A returns `404` on any of tenant B's recipient IDs (not `403`).

---

## 10. Interview talking points

- **Cursor vs offset pagination**: `OFFSET` is O(n) on the database and breaks under concurrent writes (skips/dupes); cursor is O(log n) and stable. Required for any list endpoint at scale.
- **Cache-with-pub/sub-invalidation pattern**: explain what it does *not* protect against — a stale read in the window between write and pub/sub propagation. The TTL is the bound on that window.
- **Partial unique indexes** for soft-delete: standard trick worth knowing.
- **Returning 404 for cross-tenant access**: don't leak existence. A `403` confirms the resource exists.
- **Why N+1 matters**: a recipient/channel join naively explodes into one query per recipient. Eager loading with `SELECT ... FROM recipients JOIN channels ON ...` or an explicit `IN (...)` second query keeps it bounded.

---

## 11. Implementation notes — how the built code maps to this PRD

A few deliberate deviations from the illustrative spec above, for consistency with
the rest of the service:

- **IDs are UUIDs, not `TEXT`.** The §5 DDL is illustrative; the ORM
  (`app/recipients/models.py`) uses `uuid` primary keys to match the existing
  `recipients`/`audit` modules and the dispatcher's `recipient_id` type. `channel_ids`
  is a `TEXT[]` of UUID strings so the routing snapshot round-trips through JSON.
- **`min_severity` is the severity *label*** (`'high'`), not a numeric rank — stored
  the same way alerts store severity. `app/recipients/matching.py::severity_rank`
  maps it to the §7 ranking where lower = more severe (critical = 0 … info = 4), so
  the floor check is `severity_rank(alert) <= severity_rank(sub.min_severity)`.
- **Glob matching uses `fnmatchcase`** (not `fnmatch`) so behaviour is identical
  across operating systems — plain `fnmatch` normalises case on some platforms.
- **Two-layer cache.** `app/recipients/cache.py` keeps a per-process in-memory layer
  *over* the Redis snapshot (§7). The dispatcher worker runs `listen_for_invalidations`
  to drop its local layer on the pub/sub signal; the 60s Redis TTL is the staleness
  bound if pub/sub is down. On a cache hit `resolve_targets` issues zero DB queries;
  on a miss it rebuilds in exactly two (`build_snapshot`).
- **Routing-affecting writes only.** Invalidation fires on subscription writes,
  channel deletes, and recipient deletes — not on recipient create/rename or
  unreferenced-channel adds, which can't change any matching result.
- **Tenant resolution is a v1 stand-in.** `require_tenant` reads `X-Tenant-ID` today;
  (08) replaces it with the JWT `tenant_id` claim + Postgres RLS. The contract callers
  see — cross-tenant access is invisible (`404`, never `403`) — does not change.
- **Migrations.** As with the other modules, the schema lives in the ORM; an Alembic
  revision is a follow-up (the `alembic/versions` tree is currently empty for all
  modules).
