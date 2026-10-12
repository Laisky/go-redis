package redis

import redisclient "github.com/redis/go-redis/v9"

var boundedRPush = redisclient.NewScript(`local maximum = tonumber(ARGV[1])
local trim = tonumber(ARGV[2])
local count = #ARGV - 2
if count > maximum then return redis.error_reply('RPUSH_BATCH_LIMIT') end
local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'list' then
  return redis.error_reply('WRONGTYPE bounded list requires a list')
end
local length = redis.call('LLEN', KEYS[1])
if length >= maximum or length + count > maximum then
  local keep = math.min(trim, maximum - count)
  if keep == 0 then redis.call('DEL', KEYS[1])
  else redis.call('LTRIM', KEYS[1], -keep, -1) end
end
if count > 0 then
  for first = 3, #ARGV, 128 do
    redis.call('RPUSH', KEYS[1], unpack(ARGV, first, math.min(first + 127, #ARGV)))
  end
end
return redis.call('LLEN', KEYS[1])
`)
