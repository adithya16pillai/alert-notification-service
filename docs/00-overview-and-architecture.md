# 00 — Overview & Architecture

**Owner:** Adithya
**Status:** Draft v1.0
**Stack:** Python 3.12 (FastAPI), PostgreSQL 16, Redis 7, Docker, Terraform
**Target deployment:** Single region, multi-AZ, Kubernetes (Docker Compose for local dev)

---

## 1. What we are building

A horizontally scalable service that ingests alerts from upstream producers (security tools, monitoring agents, application services), classifies them by severity, deduplicates and rate-limits per recipient, and delivers them to one or more channels (email, Slack, webhook, SMS) with at-least-once semantics and full delivery state tracking.

The system is the backbone of a SOC-style alerting workflow: a fanout layer that sits between detection systems (SIEM, IDS, custom rules) and the humans or services that need to act on those signals.

## 2. Impact & Deliverables (project-level)

### Why this exists
Alerting systems fail customers in two ways: **silent loss** (the alert never arrives) and **noise overload** (so many alerts the recipient mutes the channel). Most off-the-shelf solutions optimise for one and ignore the other. This service is designed to handle both — at-least-once delivery with dedup *and* per-recipient rate limiting — so the customer's signal-to-noise ratio is a tunable parameter, not an accident.

### Customer-facing deliverables (post-MVP)
- A public HTTP API with OpenAPI spec and producer quickstart guide.
- A self-service admin surface for managing recipients, subscriptions, and rate-limit policy.
- A documented SLA: ingest p99 < 50ms, critical alert end-to-end p95 < 5s, availability ≥ 99.95%.
- Provider integration runbooks for each supported channel.

### Internal deliverables (for engineering / ops)
- This PRD package.
- Runbooks for each failure mode (Redis loss, Postgres failover, provider outage).
- Dashboards for the four golden signals + product-specific metrics (queue depth, DLQ depth, rate-limit denials).
- Terraform modules and Helm charts that bring up the full stack from zero.
- A `BENCHMARKS.md` capturing measured numbers from load testing (filled in during Phase 5).

### Success metrics
- **Reliability**: 99.95% API availability over rolling 30 days; <0.01% alerts lost (measured by `accepted` rows without terminal `delivery_attempts`).
- **Performance**: ingest p99 < 50ms; critical alert end-to-end p95 < 5s; no severity ever starved >30s while workers have capacity.
- **Adoption**: producers onboard in <1 day with the quickstart; subscription changes go live in <5s.

## 3. Goals

1. Accept alerts via HTTP and return a tracking ID in under 50ms p99 from the caller's perspective.
2. Deliver high-severity alerts to at least one channel in under 5 seconds p95 end-to-end.
3. Guarantee at-least-once delivery with deduplication of duplicate sends within a configurable window.
4. Prevent alert fatigue via per-recipient, per-channel token-bucket rate limiting.
5. Provide a defensible audit trail for every alert and every delivery attempt.

## 4. Non-goals (explicitly out of scope for v1)

- Alert *generation* / detection logic. We are a fanout layer, not a SIEM.
- Two-way conversational channels (no ack-via-reply in v1).
- End-user UI. v1 ships an API and a minimal admin dashboard only.
- Multi-region active-active. v1 is single-region multi-AZ.

## 5. Target scale (sized honestly)

These are the numbers the design is sized for, not aspirational marketing claims. Anything beyond these requires re-sizing — explicitly called out so the design is defensible in interview.

| Dimension | Target |
|---|---|
| Steady-state ingest | 500 alerts/sec |
| Peak burst (60s window) | 5,000 alerts/sec |
| Active recipients | 50,000 |
| Channels per recipient | 1–4 |
| Retention (alert records) | 90 days hot, 1 year cold |
| Retention (delivery attempts) | 30 days |

## 6. Stack rationale

- **FastAPI** — native async I/O, OpenAPI by default, type-driven request validation via Pydantic.
- **PostgreSQL** — durable system of record; supports `SKIP LOCKED` for queue patterns, partitioning for retention, JSONB for flexible alert payloads.
- **Redis** — in-memory data structures for rate limiting (atomic Lua scripts), deduplication windows, priority queues (sorted sets), idempotency cache.
- **Docker + Compose** for local; **Kubernetes** for production. Stateless app tier; state in Postgres and Redis only.

---

## 7. Architecture

### 7.1 Logical components

```
                   ┌───────────────────────────────────────────┐
                   │            Ingestion API (FastAPI)        │
                   │  - Auth, validation, idempotency check    │
                   │  - Writes alert to Postgres (durable)     │
                   │  - Enqueues to Redis priority queue       │
                   └────────────────┬──────────────────────────┘
                                    │
                                    ▼
                   ┌───────────────────────────────────────────┐
                   │       Redis (priority queue + state)      │
                   │   ZSET per severity: critical/high/...    │
                   │   Token-bucket counters per recipient     │
                   │   Idempotency / dedup keys (TTL)          │
                   └────────────────┬──────────────────────────┘
                                    │
                                    ▼
                   ┌───────────────────────────────────────────┐
                   │         Dispatcher Workers (N)            │
                   │  - Poll queue (highest severity first)    │
                   │  - Resolve recipients & subscriptions     │
                   │  - Apply rate limit (Lua script)          │
                   │  - Fan out to channel adapters            │
                   └────────────────┬──────────────────────────┘
                                    │
        ┌───────────────────┬───────┴────────┬─────────────────┐
        ▼                   ▼                ▼                 ▼
   ┌─────────┐         ┌─────────┐      ┌─────────┐      ┌─────────┐
   │  Email  │         │  Slack  │      │ Webhook │      │   SMS   │
   │ Adapter │         │ Adapter │      │ Adapter │      │ Adapter │
   └─────────┘         └─────────┘      └─────────┘      └─────────┘
        │                   │                │                 │
        └───────────────────┴────────┬───────┴─────────────────┘
                                     ▼
                   ┌───────────────────────────────────────────┐
                   │  Postgres: delivery_attempts table        │
                   │  (status, retry_count, last_error, ...)   │
                   │  Failed jobs → DLQ stream in Redis        │
                   └───────────────────────────────────────────┘
```

### 7.2 Service boundaries (modular monolith, deployable as microservices)

We ship a single repository with strict module boundaries. Each module owns its own data and exposes a thin internal API. Real microservices add operational cost we don't need at portfolio scale, but the boundaries are clean enough that any module can be lifted into its own deployment unit without refactoring business logic.

| Module | Owns | Public surface |
|---|---|---|
| `ingestion` | Alert ingestion, idempotency, validation | HTTP `POST /v1/alerts` |
| `recipients` | Recipients, subscriptions, channels | HTTP `/v1/recipients/*` |
| `dispatcher` | Queue draining, rate limiting, routing | Background worker process |
| `channels` | Channel adapters (email/slack/webhook/sms) | Internal `Channel.send()` interface |
| `audit` | Delivery attempt log, status queries | HTTP `GET /v1/alerts/{id}/attempts` |

Each module has its own Postgres schema (`ingestion.alerts`, `recipients.recipients`, `audit.delivery_attempts`). Cross-schema reads are allowed; cross-schema writes go through the owning module's interface. No shared mutable state.

### 7.3 CAP trade-offs, per path

Documenting these explicitly because "we picked AP" is not a real answer in an interview.

| Path | Choice | Why |
|---|---|---|
| Alert ingestion (POST) | **AP** | Better to accept and queue than 503 if a replica lags. Postgres write goes to primary; if primary is unavailable we degrade to Redis-only buffer (lossy but available — opt-in via flag). |
| Idempotency check | **CP** | Must be linearizable per `idempotency_key` or we send duplicates. Implemented with `SET NX EX` in Redis (single shard, atomic). |
| Token-bucket rate limit | **CP** per recipient | A single Redis key per recipient; we accept the loss of availability if Redis is partitioned, over the (small) risk of over-sending. Cluster-mode hash-tags pin a recipient's keys to one slot. |
| Delivery state queries | **AP** with read-after-write on primary | Replicas may lag; admin UI reads from primary for status pages, replicas for analytics. |

### 7.4 Data flow for a single alert

1. Producer calls `POST /v1/alerts` with `Idempotency-Key` header.
2. Ingestion API validates payload, checks Redis idempotency key.
3. Alert row inserted into `ingestion.alerts` (status = `accepted`).
4. Alert ID pushed to `queue:alerts:{severity}` Redis ZSET (score = `submitted_at_unix_ms`).
5. API returns `202 Accepted` with alert ID. **End of synchronous path.**
6. Dispatcher worker polls queues highest-severity-first using a Lua script that pops N items atomically.
7. For each alert, dispatcher loads subscriptions for the alert's tenant/topic, applies per-recipient rate limit per channel.
8. For each (recipient × channel) that passes the limit, a `delivery_attempts` row is inserted (status = `pending`) and a job is pushed to the channel adapter's queue.
9. Channel adapter calls the external provider with timeout + retry + circuit breaker; updates `delivery_attempts.status` on completion.
10. On terminal failure after retries, the job is pushed to the DLQ Redis stream for manual inspection.

---

## 8. Repo layout

See the project [README](../README.md) for the live layout. This scaffold
implements the structure described here.

---

## 9. Where to go next

- For ingestion: `01-alert-ingestion-api.md` (TBD)
- For the queue: `02-priority-queue.md` (TBD)
- For implementation order: `10-roadmap.md` (TBD)
- For interview prep: `11-resume-and-star.md` (TBD)
