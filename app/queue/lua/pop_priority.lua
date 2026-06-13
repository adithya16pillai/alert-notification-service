-- pop_priority.lua — atomic, starvation-guarded, highest-severity-first pop.
--
-- KEYS    = severity ZSETs ordered highest priority first
--           (queue:alerts:critical, queue:alerts:high, ... queue:alerts:info).
-- ARGV[1] = batch size (max members to pop in one call)
-- ARGV[2] = starvation counter key (shared across every worker)
-- ARGV[3] = starvation N — on every Nth pop, scan lowest-severity-first
-- ARGV[4] = in-flight ZSET key (visibility tracking)
-- ARGV[5] = now (unix ms) — the server clock is never read inside the script
-- ARGV[6] = in-flight TTL (ms) — visibility timeout before the reaper re-queues
--
-- Returns a flat list of popped member ids (alert ULIDs), drained from a single
-- severity so priority is strict within one pop.
--
-- Atomicity (02 §5.2): Redis runs each script to completion single-threaded, so
-- the INCR, the ZPOPMIN drain, and the in-flight ZADD all commit as one
-- indivisible step. Two concurrent workers therefore can never observe the same
-- member — ZPOPMIN removes it before any other call can see it (acceptance: no
-- duplicate IDs under concurrent pops).

-- Numeric command args are kept as strings: redis.call requires string/integer
-- args, so we pass batch_size through verbatim and format the ZADD score.
local batch_size  = ARGV[1]
local counter_key = ARGV[2]
local N           = tonumber(ARGV[3])
local inflight    = ARGV[4]
local now_ms      = tonumber(ARGV[5])
local ttl_ms      = tonumber(ARGV[6])
local deadline    = string.format("%d", now_ms + ttl_ms)

-- Starvation guard: bump the shared counter; on every Nth pop scan from the
-- lowest severity upward, so lower queues drain at least 1-in-N even while a
-- higher severity has a standing backlog (02 §3, §5.2).
local count = tonumber(redis.call("INCR", counter_key))
local lo, hi, step
if count >= N then
  redis.call("SET", counter_key, "0")
  lo, hi, step = #KEYS, 1, -1          -- starvation tick: lowest priority first
else
  lo, hi, step = 1, #KEYS, 1           -- normal: highest priority first
end

local out = {}
for i = lo, hi, step do
  -- ZPOPMIN pops the lowest score = earliest received within this severity (FIFO).
  -- Its reply shape depends on the RESP protocol: RESP2 is a flat
  -- {member, score, member, score, ...} array; RESP3 is nested
  -- {{member, score}, ...}. Handle both so the script is client-agnostic.
  local popped = redis.call("ZPOPMIN", KEYS[i], batch_size)
  if #popped > 0 then
    local nested = (type(popped[1]) == "table")
    local stride = nested and 1 or 2
    for j = 1, #popped, stride do
      local member = nested and popped[j][1] or popped[j]
      out[#out + 1] = member
      -- Record the member in-flight with a visibility deadline. If the worker
      -- dies before it acks (ZREM), the reaper re-queues it once now > deadline.
      redis.call("ZADD", inflight, deadline, member)
    end
    return out                          -- strict priority: stop at first non-empty
  end
end

return out
