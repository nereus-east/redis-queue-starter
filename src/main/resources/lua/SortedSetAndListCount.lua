--
-- description:
-- authur: nereus east
-- date: 2020/3/21 21:07
--

local set_key = KEYS[1]
local hash_key = KEYS[2]

return redis.call('ZCARD', set_key) + redis.call('LLEN', hash_key)