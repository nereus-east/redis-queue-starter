--
-- description: 
-- author: nereus east
-- date: 2020/3/23 10:45
--

local list_key = KEYS[1]
local hash_key = KEYS[2]

local list_start = ARGV[1]
local list_end = ARGV[2]

local list_ids = redis.call('LRANGE', list_key, list_start, list_end)
if list_ids[1] ~= nil then
    return redis.call('HMGET', hash_key, unpack(list_ids))
end
return {}