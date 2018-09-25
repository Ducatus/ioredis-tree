local getChildrenArrayByLevelIndex
getChildrenArrayByLevelIndex = function (id, level, index, limit, offset, result)
  level = level - 1
  -- current level = maxlevel - level
  local value = redis.call('get', prefix .. id)
  if not value then
    return result
  end
  local list = cmsgpack.unpack(value)

  for i, v in ipairs(list) do
    local cid = v[1]
    local hasChild = v[2] -- Should be an array

    local item = { cid, hasChild }

    if hasChild ~= 0 and level ~= 0 then 
      if (cid == id) then -- If child ID == current node
        return redis.error_reply("ERR infinite loop found in 'tchildrenarray' command")
      end
      getChildrenArrayByLevelIndex(cid, level, index, limit, offset, item)
      if #item == 2 then
        v[2] = 0
      end
    end

    if level == 0 and #result ~= limit and index >= offset then -- if this is the level we're looking for
        result[#result + 1] = cid
    end

    if level == 0 then -- if this is the level we're looking for
        index = index + 1
    end

  end

  return result
end

local level = -1
local index = 0

if ARGV[2] then
  if string.upper(ARGV[2]) == 'LEVEL' then
    if not ARGV[3] then
      return redis.error_reply("ERR wrong number of arguments for 'tchildrenarray' command")
    end
    level = tonumber(ARGV[3])
  end
end

if ARGV[4] then
  if string.upper(ARGV[4]) == 'LIMIT' then
    if not ARGV[5] then
      return redis.error_reply("ERR wrong number of arguments for 'tchildrenarray' command")
    end
    limit = tonumber(ARGV[5])
  end
end

if ARGV[6] then
  if string.upper(ARGV[6]) == 'OFFSET' then
    if not ARGV[7] then
      return redis.error_reply("ERR wrong number of arguments for 'tchildrenarray' command")
    end
    offset = tonumber(ARGV[7])
  end
end

if ARGV[8] then
  if string.upper(ARGV[8]) == 'FLOWOVER' then
    if not ARGV[9] then
      return redis.error_reply("ERR wrong number of arguments for 'tchildrenarray' command")
    end
    flowover = ARGV[9]
  end
end

if level == 0 then
  return nil
end

-- getChildrenArrayByLevelIndex(
--   currentid, // ID of the node (USER SET)
--   currentlevel, // Current level reverse index doubles as max level (USER SET)
--   index, // Current index of the node in a flat array (NON-USER SET)
--   limit, // Max number of nodes to return (USER SET)
--   offset, // Offset to start placing nodes in array (USER SET)
--   flowover, // Whether to continue after hitting the maxlevel (USER SET)
--   Result) // An array  
local result = {}
getChildrenArrayByLevelIndex(id, level, index, limit, offset, result) 

while #result ~= limit and flowover == true do
    level = level + 1
    getChildrenArrayByLevelIndex(id, level, index, limit, offset, result) 
end

return result
