--
-- description:
-- Return value enumeration:
-- 0: Success
-- 1: The value is exist in the hash
-- 2: The value is exist in the sorted set

-- authur: nereus east
-- date: 2020/3/21 21:07
--

local sorted_set_key = KEYS[1]
local hash_key = KEYS[2]

local id = ARGV[1]
local content = ARGV[2]
local score = ARGV[3]

local hset_result = redis.call('HSETNX', hash_key, id, content)
if hset_result < 1 then
    return 1
end
local set_result = redis.call('ZADD', sorted_set_key, 'NX', score, id)
if set_result > 0 then
    return 0
else
    return 2
end