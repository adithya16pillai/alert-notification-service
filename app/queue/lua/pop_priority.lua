-- pop_priority.lua — atomically pop up to N items, highest-severity-first.
--
-- KEYS = ordered list of severity ZSETs, highest priority first
--        (e.g. queue:alerts:critical, queue:alerts:high, ...).
-- ARGV[1] = max items to pop (batch size)
--
-- Drains each ZSET in order by ascending score (submitted_at_ms) so within a
-- severity it is FIFO, but a higher severity always wins. Returns a flat list
-- of popped member ids. No severity is starved while higher ones have backlog;
-- the worker loop (05) bounds how long any severity waits.

local budget = tonumber(ARGV[1])
local out = {}

for i = 1, #KEYS do
  if budget <= 0 then break end
  -- ZPOPMIN pops lowest score = earliest submitted within this severity.
  local popped = redis.call("ZPOPMIN", KEYS[i], budget)
  -- popped is a flat {member, score, member, score, ...} array.
  for j = 1, #popped, 2 do
    out[#out + 1] = popped[j]
    budget = budget - 1
  end
end

return out
