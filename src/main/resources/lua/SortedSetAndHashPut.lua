--
-- description:
-- Return value enumeration:
-- 0: Add success
-- 0: Update success
-- 2: The value is not exist in the hash, but is exists in the sorted set
-- 3: The value is exist in the hash, but is not exists in the sorted set

-- authur: nereus east
-- date: 2020/3/21 21:07
--

local sorted_set_key = KEYS[1]
local hash_key = KEYS[2]

local id = ARGV[1]
local content = ARGV[2]
local score = ARGV[3]
local rollback_on_fail = ARGV[4]

local old_content = redis.call('HGET', hash_key, id);
local hset_result = redis.call('HSET', hash_key, id, content)
if hset_result > 0 then
    --add hset_result equal 1
    local set_result = redis.call('ZADD', sorted_set_key, 'NX', score, id)
    if set_result > 0 then
        return 0
    else
        if rollback_on_fail == "1" then redis.call('HDEL', hash_key, id) end
        return 2
    end
else
    --update
    local set_result = redis.call('ZADD', sorted_set_key, 'XX', 'CH', score, id)
    if set_result > 0 then
        return 1
    else
        if rollback_on_fail == "1" then redis.call('HSET', hash_key, id, old_content) end
        return 3
    end
end