--
-- description: Moves the elements in the specified score range from sorted set to list
-- Return the number of moves
-- author: nereus east
-- date: 2020/3/22 23:50
--

local sort_set_key = KEYS[1]
local list_key = KEYS[2]

local begin_score = ARGV[1]
local end_score = ARGV[2]
local limit_count = ARGV[3]

local elems
local moves
if limit_count then
    elems = redis.call('ZRANGEBYSCORE', sort_set_key, begin_score, end_score, 'LIMIT', 0, limit_count)
else
    elems = redis.call('ZRANGEBYSCORE', sort_set_key, begin_score, end_score)
end

if  not (elems[1]) then
    return 0
end

redis.call('LPUSH', list_key, unpack(elems))

if (limit_count) then
    moves = redis.call('ZREM', sort_set_key, unpack(elems))
else
    moves = redis.call('ZREMRANGEBYSCORE', sort_set_key, begin_score, end_score)
end

return moves
