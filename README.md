# alert-notification-service

A horizontally scalable service that ingests alerts, classifies them by
severity, deduplicates and rate-limits per recipient, and fans them out to
multiple channels (email, Slack, webhook, SMS) with **at-least-once** delivery
and a full audit trail.

It sits between detection systems (SIEM/IDS/custom rules) and the humans or
services that act on the signal — tuning **signal-to-noise as a parameter**, not
an accident. See [`docs/00-overview-and-architecture.md`](docs/00-overview-and-architecture.md)
for the full design, CAP trade-offs, and sizing.

## Stack

| Concern | Technology |
|---|---|
| Language | Python 3.12 |
| API framework | FastAPI |
| System of record | PostgreSQL 16 |
| Queue / rate-limit / dedup | Redis 7 |
| Migrations | Alembic |
| Local orchestration | Docker Compose |
| Production orchestration | Kubernetes |
| Infrastructure as code | Terraform |

## Repository structure

```
alert-notification-service/
├── app/
│   ├── ingestion/        # POST /v1/alerts: validate, idempotency, persist, enqueue
│   ├── queue/            # Redis priority queue + token-bucket rate limit
│   │   └── lua/          #   token_bucket.lua, pop_priority.lua (atomic)
│   ├── recipients/       # recipients, subscriptions, channel configs
│   ├── dispatcher/       # worker: drain queue, rate-limit, fan out
│   ├── channels/         # email / slack / webhook / sms adapters + circuit breaker
│   ├── audit/            # delivery_attempts log, status queries, DLQ
│   ├── auth/             # API-key auth dependency
│   ├── observability/    # logging, tracing, metrics
│   ├── errors.py         # single error hierarchy
│   ├── config.py         # Pydantic Settings (all ANS_* environment variables)
│   ├── db.py             # async SQLAlchemy engine/session
│   ├── redis_client.py   # shared Redis pool + Lua loader
│   └── main.py           # FastAPI app factory + entrypoints
├── alembic/              # migrations (per-module Postgres schemas)
├── tests/                # unit / integration / contract / load
├── infra/                # terraform/ + helm/
├── docs/                 # design documents
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
└── BENCHMARKS.md
```

## Installation

### Prerequisites

- Python 3.12+
- Docker and Docker Compose (for the containerised setup)
- PostgreSQL 16 and Redis 7 (only if running outside Docker)

### Option A — Docker Compose (recommended)

Brings up PostgreSQL, Redis, runs migrations, and starts the API and worker.

```bash
cp .env.example .env
docker compose up --build
```

The API is then available at `http://localhost:8000` (OpenAPI docs at `/docs`,
metrics at `/metrics`).

### Option B — Local Python environment

Run the datastores yourself (or point `ANS_DATABASE_URL` / `ANS_REDIS_URL` at
existing instances), then:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -e ".[dev]"
cp .env.example .env

alembic upgrade head             # apply migrations

ans-api                          # API server (uvicorn on :8000)
ans-worker                       # dispatcher worker (separate process)
```

### Running the tests

```bash
pytest
```

### Configuration

All settings are environment variables prefixed `ANS_`; see `app/config.py` and
`.env.example` for the full list and defaults.
