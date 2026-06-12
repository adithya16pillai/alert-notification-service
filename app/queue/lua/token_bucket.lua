-- token_bucket.lua — atomic per-recipient+channel rate limit (00 §7.3, CP).
--
-- KEYS[1] = bucket key (e.g. "rl:{recipient}:email"); hash-tag pins the slot
--           in cluster mode so a recipient's keys share one shard.
-- ARGV[1] = capacity (max tokens)
-- ARGV[2] = refill rate (tokens per second)
-- ARGV[3] = now (unix seconds, float) — passed in, never read from server clock
-- ARGV[4] = cost (tokens to consume, default 1)
--
-- Returns: { allowed (1|0), remaining_tokens }

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4]) or 1

local state = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

-- Refill based on elapsed wall time, capped at capacity.
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refill)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call("HSET", key, "tokens", tokens, "ts", now)
-- Expire idle buckets so cold recipients don't leak memory.
local ttl = math.ceil(capacity / refill) + 1
redis.call("EXPIRE", key, ttl)

return { allowed, tokens }
