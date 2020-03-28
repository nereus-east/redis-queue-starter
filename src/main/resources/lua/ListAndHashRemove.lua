--
-- description:
-- authur: nereus east
-- date: 2020/3/21 21:07
--

local list_key = KEYS[1]
local hash_key = KEYS[2]
local id = ARGV[1]

local list_remove_count = redis.call('LREM', list_key, 0, id);
if list_remove_count > 0 then
    redis.call('HDEL', hash_key, id)
    return 1;
end
return 0;
