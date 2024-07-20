local fio = require 'fio'
local t = require 'luatest' --[[@as luatest]]

t.before_suite(function()
	local tmpdir = assert(fio.tempdir())
	box.cfg{ memtx_dir = tmpdir, wal_dir = tmpdir, vinyl_dir = tmpdir, listen = '127.0.0.1:3301' }
	box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
end)
