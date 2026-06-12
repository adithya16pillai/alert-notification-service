# 01 — Alert Ingestion API

> **Prerequisite reading:** [00-overview-and-architecture.md](./00-overview-and-architecture.md)

---

## 1. Problem

Producers (SIEMs, monitoring agents, application services) need a fast, reliable way to submit alerts. The synchronous response must be quick — producers may be in a hot loop — but we must never lose an alert we have acknowledged with a 2xx response.

This is the contract every upstream integration depends on. If it is flaky or slow, the whole system's trust evaporates.

---

## 2. Impact & Deliverables

### Why this matters
This is the first impression for every producer integration we ever ship. Two failure modes here destroy the product:

- **Slow acks** force producers to async-fire-and-forget, which silently loses alerts during their crashes.
- **Non-idempotent endpoint** means producers cannot safely retry on network blips, forcing them to either build their own deduplication or accept double-paging — both bad outcomes.

Idempotency at the API boundary is what allows every upstream system to apply its own retry logic without coordinating with us. It is the single most important interface guarantee we make.

### Deliverables to ship
- **Public**: OpenAPI 3.1 spec for `POST /v1/alerts`, producer quickstart (curl + Python + Node examples), idempotency contract one-pager.
- **Internal**: ingestion-latency dashboard, load-test report at target RPS, runbook for "ingest latency degraded".
- **Operational**: alerting rules on p99 latency and error rate (see [09-cross-cutting-concerns.md](./09-cross-cutting-concerns.md) §1).

### Success metrics
- API availability ≥ 99.95% measured at the load balancer.
- p99 ingestion latency < 50ms (measured server-side, excluding network).
- Zero duplicate dispatches across 1M idempotency-keyed retry storms in load test.
- Producer onboarding time < 1 day using only the quickstart.

---

## 3. Functional requirements

- Accept HTTP `POST /v1/alerts` with a structured JSON payload.
- Validate the payload against a strict schema; reject malformed input with a structured error.
- Idempotency: same `Idempotency-Key` within a 24h window returns the original alert ID with `200 OK` (not a duplicate insert).
- On success, return `202 Accepted` with `{ "alert_id": "...", "status": "accepted" }`.
- Authenticated via JWT; `tenant_id` claim is required (see [08-auth-multitenancy.md](./08-auth-multitenancy.md)).

---

## 4. Non-functional requirements

- p50 latency ≤ 15ms, p99 ≤ 50ms (excluding network).
- A single instance handles 1,000 RPS on a 2-vCPU pod.
- All writes durable in Postgres **before** 202 is returned — no fire-and-pray.

---

## 5. API contract

### Request

```http
POST /v1/alerts
Authorization: Bearer <token>
Idempotency-Key: <uuid>
Content-Type: application/json

{
  "tenant_id": "acme",
  "source": "siem.splunk",
  "severity": "critical",
  "topic": "auth.brute_force",
  "title": "10+ failed logins for user admin",
  "body": "Source IP 203.0.113.42 ...",
  "labels": { "host": "web-01", "region": "eu-west-2" },
  "payload": { "raw_event": "..." },
  "occurred_at": "2026-05-17T09:12:33Z"
}
```

- `severity` ∈ {`critical`, `high`, `medium`, `low`, `info`}.
- `Idempotency-Key` is required for `POST`; format = client-supplied UUID; window = 24h.
- Unknown top-level fields are **rejected** (strict mode).

### Response — success (202)

```json
{ "alert_id": "01HXYZ...", "status": "accepted" }
```

### Response — idempotent replay (200)

Same body as above, with the original alert's ID.

### Response — error (consistent schema across the whole service)

```json
{
  "error": {
    "code": "validation_error",
    "message": "severity must be one of: critical, high, medium, low, info",
    "field": "severity",
    "trace_id": "abc-123"
  }
}
```

| HTTP | `error.code` |
|---|---|
| 400 | `validation_error` |
| 401 | `unauthorized` |
| 403 | `forbidden` |
| 409 | `idempotency_conflict` (same key, different payload) |
| 429 | `rate_limited` |
| 500 | `internal_error` |
| 503 | `service_unavailable` |

---

## 6. Data model

```sql
CREATE SCHEMA ingestion;

CREATE TABLE ingestion.alerts (
    id              TEXT PRIMARY KEY,                  -- ULID, sortable
    tenant_id       TEXT NOT NULL,
    source          TEXT NOT NULL,
    severity        TEXT NOT NULL CHECK (severity IN ('critical','high','medium','low','info')),
    topic           TEXT NOT NULL,
    title           TEXT NOT NULL,
    body            TEXT,
    labels          JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload         JSONB,
    occurred_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'accepted',  -- accepted|dispatched|completed|failed
    idempotency_key TEXT,
    deleted_at      TIMESTAMPTZ                         -- soft delete
);

CREATE INDEX idx_alerts_tenant_received ON ingestion.alerts (tenant_id, received_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX idx_alerts_severity_received ON ingestion.alerts (severity, received_at DESC) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX uq_alerts_idempotency ON ingestion.alerts (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
```

Partitioning: `received_at` range-partitioned by month for retention rollover (see [09-cross-cutting-concerns.md](./09-cross-cutting-concerns.md) §3).

---

## 7. Implementation notes

- **Validation**: Pydantic v2 models, strict mode (unknown fields rejected).
- **IDs**: ULID — sortable, doesn't leak count, plays well with Postgres B-tree indexes. Avoid UUIDv4 (random distribution kills index locality).
- **Idempotency (two-layer)**:
  1. Fast path: `SET NX EX 86400 idem:{tenant}:{key} alert_id` in Redis. If it fails, look up the existing alert and return its ID.
  2. Durable safety net: the `uq_alerts_idempotency` Postgres unique index. Belt and braces — if Redis is wiped, the DB still rejects duplicates.
- **Connection pooling**: asyncpg + PgBouncer in transaction mode. Pool size = `worker_count * 2`, never default.
- **Timeouts**: 200ms on the Postgres write, 50ms on the Redis idempotency check. Both retried once on transient errors only.
- **Enqueue order**: Postgres write first, then Redis enqueue (write-ahead-log pattern). If the Redis enqueue fails, a janitor process picks up `status='accepted'` rows older than 30s and re-enqueues them.
- **Body size**: hard cap at 256 KB. Larger payloads = `413 Payload Too Large`.

---

## 8. Acceptance criteria

- [ ] Submitting the same `Idempotency-Key` twice with the same payload returns the same alert ID, and only one row exists in `ingestion.alerts`.
- [ ] Submitting the same `Idempotency-Key` with a *different* payload returns `409 idempotency_conflict`.
- [ ] Malformed payloads return `400` with the consistent error schema and a non-empty `trace_id`.
- [ ] Killing Redis between the DB insert and the queue enqueue still results in the alert being dispatched (janitor recovery within 30s).
- [ ] Load test: 1,000 RPS sustained for 5 minutes with p99 ≤ 50ms.
- [ ] Strict validation: posting an unknown top-level field returns `400 validation_error`.
- [ ] OpenAPI spec generated from code matches the deployed behaviour (contract test).

---

## 9. Interview talking points

- **Idempotency at the API boundary**: Redis fast path + DB constraint safety net. Explain why both layers are needed — Redis for speed, Postgres for durability across Redis loss.
- **Write-ahead-log pattern** for queue enqueue: durability before notification.
- **Why ULID over UUIDv4**: monotonic-ish ordering preserves B-tree locality; UUIDv4 is uniformly random and fragments indexes.
- **Strict schema validation**: rejecting unknown fields prevents schema drift and silently-ignored typos.
- **Error schema consistency**: a single error type across endpoints simplifies client SDKs and observability — every error has a `code` and a `trace_id`.

---

## Implementation status (scaffold)

| Acceptance criterion | Status |
|---|---|
| Idempotent replay → same id, one row | ✅ Redis fast path + `_load_existing` + DB unique index |
| Same key, different payload → 409 | ✅ fingerprint compare in `service.ingest_alert` |
| Malformed → 400 + error schema + trace_id | ✅ `RequestValidationError` handler in `main.py` |
| Janitor recovery within 30s | ✅ `ingestion/janitor.py`, run from dispatcher loop |
| Strict validation (unknown field → 400) | ✅ `extra="forbid"` on `AlertIn` |
| 256 KB cap → 413 | ✅ `enforce_body_limit` dependency |
| ULID ids | ✅ `python-ulid` in `service.ingest_alert` |
| Load test p99 ≤ 50ms | ⏳ `tests/load/locustfile.py` exists; numbers TBD (BENCHMARKS.md) |
| OpenAPI contract test | ✅ `tests/contract/test_ingestion_contract.py` |
| 200-on-replay / 202-on-new | ✅ `router.post_alert` sets status from `IngestResult.replay` |

**Deferred:** JWT/`tenant_id`-claim auth (08 — currently API-key), PgBouncer pool sizing, the first Alembic migration, partition management (09 §3).
