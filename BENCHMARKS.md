# BENCHMARKS

Measured numbers from load testing (filled in during Phase 5 — see
[10-roadmap.md](./docs/10-roadmap.md)). Until then these are **targets**, not
results.

## Targets (from 00 §3 / §5)

| Metric | Target |
|---|---|
| Ingest latency (caller-perceived) | p99 < 50 ms |
| Critical alert end-to-end | p95 < 5 s |
| Steady-state ingest | 500 alerts/sec |
| Peak burst (60s) | 5,000 alerts/sec |
| API availability (rolling 30d) | ≥ 99.95% |
| Alerts lost | < 0.01% |

## Measured (TBD)

| Date | Commit | Scenario | Ingest p99 | E2E p95 | Throughput | Notes |
|---|---|---|---|---|---|---|
| — | — | — | — | — | — | not yet run |

## Method

Driven by `tests/load/locustfile.py` against a docker-compose (or staging)
stack. Capture: ingest latency histogram, dispatcher drain rate, queue depth
under sustained and burst load, rate-limit denial counts, DLQ depth.
