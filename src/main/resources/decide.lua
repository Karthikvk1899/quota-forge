-- All keys share one hash tag. Validate state before any writes: Lua has no rollback.
-- KEYS: accepted window, idempotency result, shard outbox, shard immutable policy.
-- ARGV: limit, window ms, outbox cap, request UUID, subject hash, event UUID.
local cached = redis.call('GET', KEYS[2])
if cached then return cached end
local limit, window, cap = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local expected = ARGV[1] .. ':' .. ARGV[2]
local policy = redis.call('GET', KEYS[4])
if policy and policy ~= expected then return 'POLICY_CONFLICT' end
if redis.call('LLEN', KEYS[3]) >= cap then return 'BACKLOG_FULL' end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local used = redis.call('ZCARD', KEYS[1])
local allowed = used < limit
if allowed then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  redis.call('PEXPIRE', KEYS[1], window)
  used = used + 1
end
local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local retry = 0
if not allowed then retry = math.max(1, tonumber(first[2]) + window - now) end
local result = cjson.encode({eventId=ARGV[6], requestId=ARGV[4], subject=ARGV[5],
  allowed=allowed, remaining=math.max(0, limit-used), retryAfterMs=retry,
  timestamp=now, limit=limit, windowMs=window})
redis.call('SET', KEYS[4], expected)
redis.call('SET', KEYS[2], result, 'PX', window * 2)
redis.call('RPUSH', KEYS[3], result)
return result
