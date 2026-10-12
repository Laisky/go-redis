package redis

import redisclient "github.com/redis/go-redis/v9"

var mutexAcquireScript = redisclient.NewScript(`return redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') and 1 or 0`)

var mutexRenewScript = redisclient.NewScript(`if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('PTTL', KEYS[1]) <= 0 then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])`)

var mutexReleaseScript = redisclient.NewScript(`if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])`)

var semaphoreAcquireScript = redisclient.NewScript(`local idsType = redis.call('TYPE', KEYS[1]).ok
local ownersType = redis.call('TYPE', KEYS[2]).ok
local counterType = redis.call('TYPE', KEYS[3]).ok
if (idsType ~= 'none' and idsType ~= 'zset') or (ownersType ~= 'none' and ownersType ~= 'zset') or (counterType ~= 'none' and counterType ~= 'string') then
 return redis.error_reply('WRONGTYPE semaphore state')
end
local raw = redis.call('GET', KEYS[3])
if raw and (not tonumber(raw) or tonumber(raw) >= 9007199254740991) then return redis.error_reply('INVALID semaphore counter') end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local ttl = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - ttl)
redis.call('ZINTERSTORE', KEYS[2], 2, KEYS[2], KEYS[1], 'WEIGHTS', 1, 0)
if redis.call('ZCARD', KEYS[2]) >= limit then return 0 end
local counter = redis.call('INCR', KEYS[3])
redis.call('ZADD', KEYS[2], counter, ARGV[1])
redis.call('ZADD', KEYS[1], now, ARGV[1])
return 1
`)

var semaphoreRenewScript = redisclient.NewScript(`local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local timestamp = redis.call('ZSCORE', KEYS[1], ARGV[1])
local owner = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not timestamp or not owner or tonumber(timestamp) <= now - tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], 'XX', now, ARGV[1])
return 1
`)

var semaphoreReleaseScript = redisclient.NewScript(`local ids = redis.call('ZREM', KEYS[1], ARGV[1])
local owners = redis.call('ZREM', KEYS[2], ARGV[1])
return math.max(ids, owners)
`)
