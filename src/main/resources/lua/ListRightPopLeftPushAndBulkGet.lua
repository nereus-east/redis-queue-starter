--
-- description:
-- Return value enumeration:
-- 0: Success
-- 1: The value is exist in the hash
-- 2: The value is exist in the sorted set

-- authur: nereus east
-- date: 2020/3/21 21:07
--

local pop_list_key = KEYS[1]
local push_list_key = KEYS[2]

local limit_count = ARGV[1]

local move_table = {}
for i = 1, limit_count do
    local temp = redis.call('RPOPLPUSH', pop_list_key, push_list_key);
    if not temp then
        break;
    end
    table.insert(move_table, temp)
end

return move_table