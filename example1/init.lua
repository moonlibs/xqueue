box.cfg{}
box.once('schema',function()
	box.schema.space.create('enqueued', {
		format = {
			[1] = {name = "id"},
			[2] = {name = "status"},
		},
	});
	box.space.enqueued:create_index('primary', { unique = true, parts = {1, 'num'}})
	box.space.enqueued:create_index('xq', { unique = false, parts = { 2, 'str', 1, 'num' }})
end)

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../?.lua;' .. package.path
end

local xq = require 'xqueue'

xq( box.space.enqueued, {
	-- format = {
	-- 	{name = "id", type = "unsigned"},
	-- 	{name = "status"},
	-- },
	fields = { status = 'status' };
	features = {
		id = 'time64',
	}

} )

require'console'.start()
os.exit()
