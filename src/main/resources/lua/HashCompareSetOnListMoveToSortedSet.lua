--
-- description:
-- author: nereus east
-- date: 2020/3/26 9:05
--
local list_key = KEYS[1]
local sorted_set_key = KEYS[2]
local hash_key = KEYS[3]

local list_remove_count = ARGV[5]
local ids = cjson.decode(ARGV[1])
local scores = cjson.decode(ARGV[2])
local old_contents = cjson.decode(ARGV[3])
local contents = cjson.decode(ARGV[4])

local add_sorted_table = {}
local set_hash_table = {}
for index, id in ipairs(ids) do
    if (redis.call('HGET', hash_key, id) == old_contents[index]) then
        local remove_count = redis.call('LREM', list_key, list_remove_count, id);
        if remove_count > 0 then
            table.insert(add_sorted_table, scores[index])
            table.insert(add_sorted_table, id)
            table.insert(set_hash_table, id)
            table.insert(set_hash_table, contents[index])
        end
    end
end
if #add_sorted_table == 0 then return 0 end;
redis.call('ZADD', sorted_set_key, unpack(add_sorted_table))
redis.call('HMSET', hash_key, unpack(set_hash_table))
return #add_sorted_table / 2
