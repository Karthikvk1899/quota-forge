-- Deduplication and projection update share a Redis Cluster slot.
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
redis.call('LPUSH', KEYS[3], ARGV[2])
redis.call('LTRIM', KEYS[3], 0, 99)
redis.call('SET', KEYS[1], '1', 'EX', 691200)
return 1
