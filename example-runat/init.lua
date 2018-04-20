box.cfg{
	listen = '127.0.0.1:4221'
}
box.once('access:v1', function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

require 'strict'.on()

-- box.once('schema',function()
	local format = {
			[1] = {name = "id", type = "string"},
			[2] = {name = "status", type = "string"},
			[3] = {name = "run", type = "number"},
			[4] = {name = "other", type = "number"},
		}
	local r,e = box.schema.space.create('enqueued', {
		format = format,
		if_not_exists = true
	})
	if e ~= 'created' then
		box.space.enqueued:format(format)
	end

	box.space.enqueued:create_index('primary', { unique = true, parts = {1, 'str'}, if_not_exists = true})
	box.space.enqueued:create_index('xq', { unique = false, parts = { 2, 'str', 1, 'str' }, if_not_exists = true})
	box.space.enqueued:create_index('run', { unique = true, parts = { 3, 'number', 1, 'str' }, if_not_exists = true})
-- end)

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../?.lua;' .. package.path
end

local xq = require 'xqueue'

xq( box.space.enqueued, {
	-- format = {
	-- 	{name = "id", type = "unsigned"},
	-- 	{name = "status"},
	-- },
	fields = {
		status = 'status';
		runat = 'run'
		-- runat = 'other';
	};
	features = {
		-- id = 'time64',
		delayed = true,
		ttr = 10,
		ttl = true,
	};
	debug = true

} )

N = require 'net.box'.connect(box.cfg.listen)
N:call('box.space.enqueued:take',{1})
N:close()
N=nil

require'console'.start()
os.exit()
