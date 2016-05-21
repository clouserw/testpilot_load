-- The Universal Search project discards some recorded data in an effort to
-- preserve users' privacy:
--
--  *  Any queries that have only been made once.
--
--  *  Any queries longer than 6 characters that have been made less than 10%
--  as frequently as the most common query of that length (e.g. if the most
--  frequently-made 8-character query has been made 1000 times, all 8-character
--  queries made fewer than 100 times will be discarded).
--
--  See full details at
--  https://github.com/mozilla/universal-search/blob/master/docs/metrics.md

package.path = package.path .. ';../utils/?.lua'
local utils = require("utils")

local debug = true

-- A little hacky, but seems cleaner than passing the connection through every
-- function call
local env, con = utils.opendb()


-- Permanently removes rows from the database which appear less than a certain threshold
--
-- @date which date to look up. Must be in the format YYYYMMDD
-- @length an integer, what size queries to look for
-- @threshold an integer, any query used fewer times than this will be removed
local function delete_infrequently_used_queries(date, length, threshold)

  -- TODO make this DELETE FROM instead of SELECT
  local query = "SELECT query " ..
        " FROM universalsearch_server_%s" ..
        " WHERE LENGTH(query) = %i" ..
        " GROUP BY query" ..
        " HAVING count(query) > %i"

  -- Prepared statements weep ;_;
  local q = string.format(query, date, length, threshold)

  if debug then print("  Running query: " .. q) end

  local ok, cur, err = pcall(con.execute, con, q)
  if err or not ok then
    print(string.format("Failure!\nQUERY: %s\nERROR: %s", q, tostring(err)))
    return nil
  end

  -- TODO docs say on a DELETE cur has "the num of rows affected" but I don't know what that looks like yet...
  return 0

end

-- Looks up all queries in the database of a certain length and returns the
-- count of the most used query.
--
-- @date which date to look up. Must be in the format YYYYMMDD
-- @length an integer, what size queries to look for
local function get_most_used_query_count(date, length)

  local max = nil

  local query = "SELECT query, " ..
                "       COUNT(query) AS max_query_count " ..
                " FROM universalsearch_server_%s " ..
                " WHERE LENGTH(query) = %i " ..
        " GROUP BY query " ..
        " ORDER BY max_query_count " ..
        " DESC LIMIT 1"

  -- Prepared statements weep ;_;
  local q = string.format(query, date, length)

  if debug then print("  Running query: " .. q) end

  local ok, cur, err = pcall(con.execute, con, q)
  if err or not ok then
    print(string.format("Failure!\nQUERY: %s\nERROR: %s", q, tostring(err)))
    return nil
  end

  -- only 1 row comes back
  row = cur:fetch({}, "a")
  max = row.max_query_count
  cur:close()

  return max

end


local function main()

  if not env then
    print("Database failed to connect. Bailing...")
    return nil
  end

  -- Default to processing yesterday.  YYYYMMDD format.
  local date = os.date('%Y%m%d', os.time() - 24 * 60 * 60)

  -- Just a simple sanity test on the date.  Lua doesn't support ^[0-9]{8}$
  if arg[1] and string.match(arg[1], '^%d%d%d%d%d%d%d%d$') then
    date = arg[1]
  end

  -- Anything larger than 20 is discarded before it hits the database, so we
  -- are only worried about results between 6 and 20
  local n = 6
  while (n < 20)
  do
    max = get_most_used_query_count(date, n)
    if max then
      print(string.format("The most frequent (%i) letter query on (%s) was used (%i) times", n, date, max))
      local threshold = math.ceil(max * .1)
      if threshold > 0 then
    d = delete_infrequently_used_queries(date, n, threshold)
        print(string.format("We deleted (%i) queries which were used fewer than (%i) times.", d, threshold))
      end
    end
    n = n+1
  end

end

main()

utils.closedb(env, con)
