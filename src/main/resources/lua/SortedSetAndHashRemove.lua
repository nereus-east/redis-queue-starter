--
-- description:
-- authur: nereus east
-- date: 2020/3/21 21:07
--

local zset_key = KEYS[1]
local hash_key = KEYS[2]

local rem_zset_count = redis.call('ZREM', zset_key, unpack(ARGV));
local rem_hash_count = redis.call('HDEL', hash_key, unpack(ARGV));
return { rem_zset_count, rem_hash_count }