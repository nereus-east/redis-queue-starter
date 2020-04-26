--
-- description:
-- authur: nereus east
-- date: 2020/4/2 10:30
--

local list_key = KEYS[1]
local hash_key = KEYS[2]

local ids = cjson.decode(ARGV[1])
local list_remove_count = ARGV[2]

local removeIds = {}

for index, id in ipairs(ids) do
    local remove_count = redis.call('LREM', list_key, list_remove_count, id);
    if remove_count > 0 then
        table.insert(removeIds, id)
    end
end

if #removeIds == 0 then return 0 end

redis.call('HDEL', hash_key, unpack(removeIds))
return #removeIds;
