# 02 — Severity-Based Priority Queue

> **Prerequisite reading:** [00-overview-and-architecture.md](./00-overview-and-architecture.md)
>
> **Status:** Implemented. Code: `app/queue/priority_queue.py`,
> `app/queue/lua/pop_priority.lua`, `app/dispatcher/worker.py`. Tests:
> `tests/integration/test_priority_queue_redis.py`, `tests/unit/test_priority_queue.py`.

---

## 1. Problem

A `critical` alert sitting behind a 10,000-item backlog of `info` alerts defeats the purpose of severity. We need strict priority ordering — but pure priority causes lower severities to starve forever during sustained high-severity traffic. The design must guarantee both.

---

## 2. Impact & Deliverables

### Why this matters
Severity is the feature customers pay tiered pricing for. "Critical" has to *mean* something measurable — specifically, that during a noisy window the critical signal is dispatched first, every time. Without this, severity is just a label and tiered pricing falls apart.

Starvation prevention is the less-glamorous half, but it is the difference between "we deliver every alert" and "we deliver only the loudest." A customer's `low`-severity weekly digest must not sit in the queue forever because their `critical` channel is busy — they paid for both.

### Deliverables to ship
- **Customer-facing**: Priority semantics one-pager — "what `critical` means, what it guarantees."
- **Engineering**: this design note + the starvation guarantee, the Lua script, and its proof of atomicity (§5.4).
- **Operational**: queue-depth-by-severity dashboard (`ans_queue_depth{severity}`), critical-time-to-dispatch histogram (`ans_critical_ttd_seconds`), in-flight depth (`ans_inflight_depth`).
- **Test artifact**: load test results showing critical TTD under various backlog depths (`tests/load/locustfile.py`, captured in `BENCHMARKS.md`).

### Success metrics
- Critical alert time-to-dispatch (TTD) < 1s under any backlog depth up to 10k.
- No severity stays in the queue >30s when workers have spare capacity.
- Zero double-processing of the same alert across worker concurrency tests.

---

## 3. Functional requirements

- Five severity levels: `critical`, `high`, `medium`, `low`, `info`.
- Higher severity is **always** drained before lower severity, with a starvation guard: at least 1 in every N pops must come from a non-empty lower queue (N configurable via `ANS_QUEUE_STARVATION_FACTOR`, default 10).
- Within a severity, FIFO ordering by `received_at`.
- Pop operations are atomic — no two workers process the same alert.

---

## 4. Non-functional requirements

- A `critical` alert enqueued during a 10,000-item `info` backlog must be picked up by a worker within 1 second.
- Pop latency p99 ≤ 5ms for a worker requesting up to 50 items.

---

## 5. Design

### 5.1 Data structures

Five Redis sorted sets, one per severity:

```
queue:alerts:critical
queue:alerts:high
queue:alerts:medium
queue:alerts:low
queue:alerts:info
```

- Score = `received_at` Unix milliseconds.
- Member = alert ID (ULID).

Lowest score = oldest = next to pop, so `ZPOPMIN` gives FIFO within a severity in constant time. Two auxiliary keys complete the picture:

```
queue:alerts:starvation_counter   # INT, shared pop counter for the 1-in-N guard
queue:alerts:inflight             # ZSET, member=alert_id score=visibility deadline (ms)
```

### 5.2 The Lua script (`pop_priority.lua`)

Workers pop via a single Lua script (atomic, one round-trip). It:

1. `INCR`s the shared starvation counter.
2. If the counter reached N, resets it to 0 and scans **lowest-severity-first** (the starvation tick); otherwise scans **highest-first** (the normal path).
3. `ZPOPMIN`s up to `batch_size` from the first non-empty queue in that scan order, and **returns from that one severity only** — so priority is strict within a single pop.
4. For every popped member, `ZADD`s it to the in-flight ZSET with score `now + visibility_ttl`.

```
KEYS    = the five queue keys, highest priority first
ARGV[1] = batch_size
ARGV[2] = starvation_counter_key
ARGV[3] = starvation N
ARGV[4] = inflight_key
ARGV[5] = now (unix ms; passed in, never read from the server clock)
ARGV[6] = visibility TTL (ms)
```

Loaded once via `register_script` (→ `SCRIPT LOAD`) and invoked by hash with `EVALSHA`.

**RESP2/RESP3 robustness.** `ZPOPMIN … count` returns a *flat* `{m, s, m, s}` array under RESP2 but a *nested* `{{m, s}, …}` array under RESP3. The script detects the shape (`type(popped[1]) == "table"`) and extracts members for both, so it is client-protocol agnostic. Numeric command arguments are passed as strings/`string.format("%d", …)` because `redis.call` requires string args.

### 5.3 Worker polling loop

The dispatcher (`app/dispatcher/worker.py`) pops a batch highest-severity-first, processes each alert, and **acks** it out of the in-flight set once it is durably handled. An alert whose processing raises is left in-flight and recovered by the reaper (§6). When a pop returns nothing it sleeps `worker_poll_interval_ms` (default 100ms) before retrying.

We don't use `BZPOPMIN` (blocking pop) because it works on a single key — we need multi-key priority. Polling with a short interval gives bounded idle CPU with sub-second wake-up on new traffic.

### 5.4 Proof of atomicity

> **Claim:** no two workers ever receive the same alert ID from `pop_priority`.

Redis executes a script to completion on its single command-processing thread; no other command (from any client) interleaves. Within one invocation the only operation that *removes* a member from a queue is `ZPOPMIN`, which atomically returns and deletes the lowest-scored members. Once worker A's script has run `ZPOPMIN` on a key, those members are gone from the keyspace before any other client — including worker B's script — can observe the key again. Therefore worker B's `ZPOPMIN` cannot return them. The in-flight `ZADD` happens inside the same uninterrupted script, so a member is *either* still in its queue *or* in the in-flight set, never both and never neither. ∎

This is verified empirically by `test_no_duplicate_ids_under_concurrent_pops` (4 workers × 40 pops over 100 alerts → exactly 100 unique IDs).

---

## 6. Implementation notes

- **Visibility / in-flight set.** After pop, members live in `queue:alerts:inflight` scored by a deadline (`now + ANS_INFLIGHT_TTL_SECONDS`, default 60s). The dispatcher `ZREM`s on success (`ack_inflight`). A reaper (`reap_inflight`, run on the same cadence as the ingestion janitor) reads expired members (`ZRANGEBYSCORE 0 now`), and — using **Postgres as the source of truth** — re-enqueues only those still `status='accepted'` at their original `received_at` score (front of their severity), then clears them from in-flight. A worker that made progress has already flipped the row to `dispatched`/terminal, so a finished-but-unacked alert is simply forgotten rather than re-delivered. This gives at-least-once semantics borrowed from SQS's visibility-timeout pattern.
- **Backpressure.** If `queue:alerts:info` exceeds `ANS_INFO_SHED_THRESHOLD` (default 100k), ingestion can reject `info` with `503 backpressure_shed` (`ANS_INFO_SHED_ENABLED`, off by default — opt-in per-tenant policy in v2). The check runs **before** any durable work and only for `info`, so the hot path pays one `ZCARD` only when it might actually reject. Critical is never shed.
- **Cluster mode.** If Redis is sharded, keep the five queue keys + the counter + in-flight key on one shard via a hash tag (`queue:alerts:{global}:critical`, etc.); the Lua script requires all keys on the same node. v1 runs single-node, so the plain prefix is used.
- **Multi-tenant fairness (v2).** Replicate the starvation counter per tenant (`starvation:{tenant_id}`) so a noisy tenant can't break the guarantee for others. v1 uses one global counter — sufficient for portfolio scale.

---

## 7. Acceptance criteria

- [x] With 10,000 `info` items pre-loaded, an enqueued `critical` is consumed first — `test_critical_jumps_a_large_info_backlog`.
- [x] FIFO within a severity by `received_at` — `test_fifo_within_a_severity`.
- [x] Starvation guard surfaces a `low` item by the Nth pop while `critical` stays full — `test_starvation_guard_surfaces_low_within_n_pops`.
- [x] Concurrent workers never both process the same alert ID — `test_no_duplicate_ids_under_concurrent_pops`.
- [x] Pop tracks the alert in-flight; ack clears it — `test_pop_tracks_inflight_and_ack_clears_it`.
- [x] Worker crash mid-batch: in-flight items still `accepted` are re-queued, finished ones are forgotten — `test_reaper_requeues_expired_accepted_only`.

---

## 8. Interview talking points

- **Why sorted sets over Redis Streams**: we want per-severity FIFO with multi-key priority. Streams give consumer groups and replay but cost more memory and don't help with cross-key priority.
- **Trade-off vs Kafka**: Kafka would give partitioned ordering and replay, but at the cost of a much heavier ops surface and (with 5 priority topics) coordination complexity. At our scale (5k/sec burst), Redis Lua is sufficient. **If we crossed 50k/sec or needed multi-day replay, Kafka becomes justified.**
- **Starvation prevention as a correctness feature**: without it, the system makes promises it can't keep about low-severity delivery. The 1-in-N rule is deliberately simple — easy to reason about, tune, and verify in tests.
- **Why polling instead of blocking pops**: `BZPOPMIN` only takes one key; we have five. Polling with a short interval is the standard workaround and the idle-latency cost is acceptable.
- **Visibility timeout pattern**: borrowed from SQS — demonstrates at-least-once queue semantics, with Postgres as the tie-breaking source of truth so the reaper never double-delivers finished work.
