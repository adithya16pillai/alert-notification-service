# Outstanding Deliverables Tracker

> A single, honest inventory of everything the PRDs (`00`–`04`) call for that is
> **not** shippable from code alone — docs, dashboards, runbooks, infra,
> benchmarks — plus code that was deliberately **deferred**.
>
> The per-feature PRDs each have their own "Acceptance criteria" / "Implementation
> status" sections for the *code*. This file collects the **non-code** and
> **deferred** items so nothing falls through the cracks between features.
>
> Legend: ✅ done · 🟡 partial / scaffold · ⬜ not started · ⏳ blocked on something else

---

## How to read this

Each feature's *core code* is largely implemented (see each PRD's status table).
What remains clusters into five recurring buckets that no amount of application
code produces on its own:

1. **Customer-facing docs** — OpenAPI specs, quickstarts, SLAs, references.
2. **Internal docs** — interface specs, runbooks, test reports.
3. **Operational** — dashboards, alerting rules, circuit-breaker panels.
4. **Infra & benchmarks** — Terraform, Helm, measured load-test numbers.
5. **Deferred code** — work consciously pushed to a later phase or another PRD.

---

## 0. Project-level (`00-overview-and-architecture.md`)

### Customer-facing (post-MVP)
| Item | Status | Notes |
|---|---|---|
| Public HTTP API + OpenAPI spec | 🟡 | FastAPI auto-generates OpenAPI; no published/curated spec artifact or versioning story. |
| Self-service **admin UI** for recipients/subscriptions/rate-limit policy | ⬜ | API exists (`03`); no UI. Explicit v1 non-goal (00 §4) but a customer deliverable. |
| Documented **SLA** (ingest p99 < 50ms, critical e2e p95 < 5s, availability ≥ 99.95%) | ⬜ | Targets live in `BENCHMARKS.md`; no signed SLA doc. |
| Provider integration runbooks (per channel) | ⬜ | See `04` operational items below. |

### Internal / ops
| Item | Status | Notes |
|---|---|---|
| Failure-mode runbooks: Redis loss, Postgres failover, provider outage | ⬜ | None written. Highest-value ops gap. |
| Dashboards: four golden signals + product metrics (queue depth, DLQ depth, rate-limit denials) | ⬜ | Metrics **are emitted** (`app/observability/metrics.py`); no dashboard JSON / Grafana provisioning. |
| Terraform modules (network, postgres, redis, cluster, observability) | 🟡 | `infra/terraform/README.md` lists modules; "Not yet implemented — scaffold only." |
| Helm charts (api + worker deployments, HPA, ConfigMap/Secret) | 🟡 | `infra/helm/README.md` scaffold only. |
| `BENCHMARKS.md` filled with measured numbers (Phase 5) | 🟡 | File exists with **targets**; "Measured (TBD)" table empty. |

### Success-metric measurement
| Item | Status | Notes |
|---|---|---|
| Reliability: <0.01% alerts lost (accepted rows w/o terminal `delivery_attempts`) | ⬜ | Needs a measurement query/job + dashboard. |
| Performance: ingest p99 < 50ms, critical e2e p95 < 5s | ⬜ | Needs load test run (see benchmarks). |
| Adoption: producer onboarding < 1 day | ⬜ | Needs the quickstart (`01`) to exist first. |

### Referenced-but-missing PRDs
`07` (retry / circuit breaker / DLQ), `08` (auth & multitenancy), `09` (cross-cutting:
observability, partitioning, secret-ref resolution), `10` (roadmap), `11` (resume / STAR)
are linked across the docs but **do not exist yet**. Several deferred items below
are blocked on these. ⬜

---

## 1. Alert Ingestion (`01-alert-ingestion-api.md`)

### Docs
| Item | Status | Notes |
|---|---|---|
| OpenAPI 3.1 spec for `POST /v1/alerts` (curated/published) | 🟡 | Auto-generated; not published or contract-pinned beyond the in-repo contract test. |
| Producer quickstart — curl + Python + Node examples | ⬜ | Onboarding-<1-day metric depends on this. |
| Idempotency contract one-pager | ⬜ | The single most important interface guarantee (01 §2) — undocumented for customers. |

### Operational
| Item | Status | Notes |
|---|---|---|
| Ingestion-latency dashboard | ⬜ | `ans_http_request_latency_seconds` emitted; no dashboard. |
| Load-test report at target RPS (1k RPS, 5 min, p99 ≤ 50ms) | ⬜ | `tests/load/locustfile.py` exists; not run, numbers TBD. |
| Runbook: "ingest latency degraded" | ⬜ | |
| Alerting rules on p99 latency + error rate (09 §1) | ⏳ | Blocked on `09`. |

### Deferred code
- JWT / `tenant_id`-claim auth — currently API-key only (blocked on `08`). ⏳
- PgBouncer transaction-mode pool sizing (`worker_count * 2`). ⬜
- First **Alembic migration** (schema currently lives only in the ORM). ⬜
- Monthly partition management for `ingestion.alerts.received_at` (09 §3). ⏳

---

## 2. Severity-Based Priority Queue (`02-severity-based-priority-queue.md`)

> Core code & acceptance tests: ✅ (see PRD §7 — all boxes checked).

### Docs
| Item | Status | Notes |
|---|---|---|
| Priority-semantics one-pager — "what `critical` guarantees" | ⬜ | Customer-facing; ties to tiered pricing story. |

### Operational
| Item | Status | Notes |
|---|---|---|
| Queue-depth-by-severity dashboard (`ans_queue_depth{severity}`) | ⬜ | Metric emitted; no dashboard. |
| Critical TTD histogram panel (`ans_critical_ttd_seconds`) | ⬜ | Metric emitted; no panel. |
| In-flight depth panel (`ans_inflight_depth`) | ⬜ | Metric emitted; no panel. |

### Benchmarks
| Item | Status | Notes |
|---|---|---|
| Load-test: critical TTD vs backlog depth | ⬜ | Captured in `BENCHMARKS.md` once run. |

### Deferred code (v2, per PRD §6)
- Per-tenant starvation counter (`starvation:{tenant_id}`) for multi-tenant fairness. ⬜
- Redis cluster-mode hash-tag key pinning (v1 is single-node). ⬜

---

## 3. Recipient & Subscription Management (`03-recipient-and-subscription-management.md`)

> Core code & acceptance tests: ✅ (see PRD §9 — all boxes checked).

### Docs
| Item | Status | Notes |
|---|---|---|
| Admin REST API OpenAPI docs (curated) | 🟡 | Auto-generated only. |
| **RBAC matrix** — who can edit what | ⏳ | Blocked on `08` (auth). |
| Subscription pattern reference — glob syntax + examples | ⬜ | `auth.*` etc.; behaviour implemented (`matching.py`), undocumented for customers. |

### Internal / compliance
| Item | Status | Notes |
|---|---|---|
| Tenant-isolation **test report** (every endpoint exercised cross-tenant) | 🟡 | Cross-tenant 404 covered in contract tests; no consolidated report, and DB-level RLS not yet in place. |
| Subscription-cache-hit-rate dashboard | ⬜ | `ans_subscription_cache_ops_total{result}` emitted; no dashboard. |
| Data-access audit-log spec (who changed which subscription, when) | ⬜ | Compliance deliverable; not designed. |

### Deferred code
- Postgres **RLS** policies for tenant scoping (today app-layer `X-Tenant-ID` stand-in; blocked on `08`). ⏳
- Channel **verification workflow** (`verified` flag; send test + confirm) — v2. ⬜
- Alembic migration for the `recipients` schema. ⬜
- Soft-delete cascade trigger (currently service-level). ⬜

---

## 4. Multi-Channel Delivery (`04-multi-channel-delivery.md`) — *this feature*

> Core code: ✅ adapters (email/Slack/webhook/SMS), classification, per-channel
> policy + jittered backoff, sandboxed Jinja2 templating, secrets resolver,
> per-channel circuit breaker, contract test suite. See the implementation summary.

### Customer-facing docs
| Item | Status | Notes |
|---|---|---|
| Per-channel **SLA table** (latency, retry budget, supported features) | ⬜ | Policy values live in `app/channels/policy.py` (§5); not turned into a customer SLA doc. |
| Supported-channel **matrix** | ⬜ | |
| "Adding a new channel" guide (for v2 producers) | ⬜ | Distinct from the internal checklist below. |

### Internal docs
| Item | Status | Notes |
|---|---|---|
| Channel adapter **interface spec** (one page) | ⬜ | Code + docstrings exist (`app/channels/base.py`); no standalone one-pager. |
| Provider-failure **runbook per channel** (SES/Slack/Twilio/webhook) | ⬜ | |
| Channel-addition **checklist** (code, tests, docs, secrets) | ⬜ | Needed to prove the "≤ 1 day to add a channel" criterion. |

### Operational
| Item | Status | Notes |
|---|---|---|
| Per-channel error-rate dashboard | ⬜ | `ans_delivery_errors_total{channel,classification}` now emitted; no dashboard. |
| Per-provider circuit-breaker state panel | ⬜ | `ans_circuit_breaker_state{channel}` now emitted; no panel. |

### Acceptance criteria still unproven (PRD §10)
| Item | Status | Notes |
|---|---|---|
| **Chaos test**: Slack outage → email/webhook unaffected, Slack circuit opens within 30s | ⬜ | Breaker isolation is unit-tested; the end-to-end chaos test is not built. |
| **CI credential-leakage grep**: inject bad creds, grep logs for secrets | ⬜ | Resolver logs names not values by design; the CI guard test is not written. |
| Webhook signature verified **on a receiver** | 🟡 | Signing + self-verification tested; no receiver-side reference verifier shipped. |
| "Add a fictional channel in ≤ 1 day" (intern test) | ⬜ | Depends on the channel-addition checklist. |

### Deferred code
- **AWS Secrets Manager backend** is a lazy stub (`app/channels/secrets.py::AwsSecretsBackend`) — needs `boto3`, real wiring, and tests; `env` backend is the only exercised path. ⬜
- **Live provider integration**: no end-to-end send against real SES/Slack/Twilio (no creds in this env); adapters are validated via mock transport / stubbed SMTP only. ⬜
- **`audit.delivery_attempts` schema gap vs PRD §7**: the ORM model is simpler than the spec — missing `channel_id`, `channel_kind`, `started_at`, `completed_at`, and **monthly range partitioning + S3 cold-storage export**. No Alembic migration exists. ⬜
- **Tenant-level template overrides**: the renderer's fallback chain looks up `…/templates/tenant/<tenant>/<severity>.j2` first, but no tenant templates are shipped and there's no API to manage them. 🟡
- **`Retry-After` HTTP-date form**: only delta-seconds is parsed; HTTP-date falls back to computed backoff. 🟡
- **Full retry/DLQ orchestration** (long backoffs without blocking the worker) is intentionally left to `07`; the dispatcher currently does in-line retries. ⏳

---

## 5. Per-Recipient Token Bucket Rate Limiting (`05-...`) — *this feature*

> Core code: ✅ atomic token-bucket Lua (pre-existing), per-(tenant, recipient,
> channel) policy overrides with a cached resolver + config API, critical
> bypass, and **defer-instead-of-drop** (per-severity retry ZSET + retry worker
> + 60s cap → DLQ `abandoned`). All §8 acceptance criteria covered by tests
> (`tests/integration/test_rate_limit_redis.py`, `tests/unit/test_rate_limit_*`).

### Customer-facing docs
| Item | Status | Notes |
|---|---|---|
| Rate-limit configuration **API** | ✅ | `PUT/GET/DELETE /v1/rate-limit-policies` (`app/recipients/router.py`). |
| Rate-limit configuration **UI** | ⬜ | Part of the absent admin UI (`00`). |
| Per-tenant default-policy doc | ⬜ | Default (10 tokens, 1/s, critical-bypass-on) lives in `config.py`; undocumented for customers. |
| "What gets rate-limited and what doesn't" reference | ⬜ | Critical-bypass + per-channel independence not written up. |

### Internal / operational
| Item | Status | Notes |
|---|---|---|
| Rate-limit-denial dashboard (per recipient, per channel) | 🟡 | `ans_rate_limit_denials_total{channel}` + `ans_rate_limit_deferred_total` / `_abandoned_total` + `ans_retry_queue_depth{severity}` emitted; **per-recipient** breakdown is deliberately off-metrics (cardinality) and must come from logs. No dashboard built. |
| Lua proof-of-atomicity note | 🟡 | Verified by the 50-worker test; no written note like the queue's §5.4. |
| Alert on sustained per-recipient denials (policy review) | ⏳ | Needs the log-based per-recipient signal + an alerting rule (`09`). |

### Deferred code
- **Policy cache invalidation is TTL-only** (no pub/sub): a policy edit propagates to other workers within `rate_limit_policy_cache_ttl_seconds` (60s), unlike subscriptions' <1s. Acceptable for slower-moving config; upgrade to pub/sub if needed. 🟡
- **`rate_limit_policies` has no Alembic migration** (schema lives in the ORM, like every other module). ⬜
- **`Retry-After`-style external coordination** for deferral is in-process only; a parked delivery lives in Redis and is retried by whichever worker drains it (correct), but there's no per-recipient fairness across the retry queue (v2, mirrors the queue's per-tenant fairness gap). ⬜
- **Benchmark**: rate-limit decision p99 ≤ 2ms (§4) not yet measured (`BENCHMARKS.md`). ⬜

---

## Cross-cutting themes (worth tackling once, not per-feature)

1. **Dashboards** — every feature emits Prometheus metrics but ships **zero**
   dashboards. One Grafana provisioning bundle covers `00`/`01`/`02`/`03`/`04`/`05`.
2. **Runbooks** — Redis loss, Postgres failover, provider outage, ingest-latency,
   per-channel provider failure. A single runbook directory.
3. **Alembic migrations** — *no module has a migration*; schemas live only in the
   ORM. One migration pass covers `ingestion`, `recipients`, `audit`.
4. **Auth (`08`)** — JWT + `tenant_id` claim + Postgres RLS unblocks the RBAC
   matrix, the tenant-isolation report, and replaces the `X-Tenant-ID` / API-key
   stand-ins across `01` and `03`.
5. **Benchmarks (`BENCHMARKS.md`)** — one load-test run populates the ingest,
   queue, and channel-latency numbers that three PRDs depend on.
6. **Infra (Terraform + Helm)** — both are README scaffolds; required for the
   "bring up the stack from zero" deliverable (`00`).
7. **Quickstarts & OpenAPI** — curated, published API docs + producer quickstart
   drive the "<1 day onboarding" adoption metric.

---

_Last updated for the `04 — Multi-Channel Delivery` implementation. Update the
relevant rows as items land, and move fully-done features out of this tracker._
