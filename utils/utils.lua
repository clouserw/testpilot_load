package.path = package.path .. ';../hindsight/io_modules/?.lua'
package.cpath = package.cpath .. ';../hindsight/io_modules/?.so'

-- load database configuration
dofile("../hindsight/hs_run/output/universalsearch_endp_redshift.cfg")

local driver = require "luasql.postgres"

local utils = {}

function utils.opendb()
  local conn
  local env, err = driver.postgres()
  if env then
    conn, err = env:connect(db_config.dbname, db_config.user,
                    db_config._password, db_config.host,
                db_config.port)
    if conn then
      return env, conn
    end
    env:close()
  end
  trace("Opening db failed", name, err)
  return nil, err
end


function utils.closedb(env, conn)
  if env then
    if conn then
      conn:close()
    end
    env:close()
  end
end

return utils
