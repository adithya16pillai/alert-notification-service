-- pop_retry.lua — atomically drain *due* deferred deliveries, highest severity
-- first (05 §7). Mirrors the priority-queue contract: a single script does the
-- range-then-remove so two retry workers never pop the same parked delivery.
--
-- KEYS    = the per-severity retry ZSET keys, highest priority first
-- ARGV[1] = now (unix ms) — passed in, never read from the server clock
-- ARGV[2] = limit (max items to return this call)
--
-- Returns: a flat array of the popped member strings (JSON-encoded deliveries).

local now   = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local out = {}
local remaining = limit

for i = 1, #KEYS do
  if remaining <= 0 then break end
  -- Members whose due-time has arrived (score <= now), oldest first.
  local due = redis.call("ZRANGEBYSCORE", KEYS[i], "-inf", now, "LIMIT", 0, remaining)
  for j = 1, #due do
    redis.call("ZREM", KEYS[i], due[j])
    out[#out + 1] = due[j]
  end
  remaining = remaining - #due
end

return out
