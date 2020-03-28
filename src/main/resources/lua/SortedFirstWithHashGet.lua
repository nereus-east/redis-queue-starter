--
-- description: 
-- author: nereus east
-- date: 2020/3/23 10:45
--

local set_key = KEYS[1]
local hash_key = KEYS[2]

local set_first_value_table = redis.call('ZRANGE', set_key, 0, 0);
local set_first_value = set_first_value_table[1];
if set_first_value then
    return redis.call('HGET', hash_key, set_first_value)
end
return nil