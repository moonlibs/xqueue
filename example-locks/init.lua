box.cfg{
	-- listen = '127.0.0.1:4221'
}
-- box.once('access:v1', function()
--         box.schema.user.grant('guest', 'read,write,execute', 'universe')
-- end)

require 'strict'.on()

-- box.once('schema',function()
	local format = {
		{name = "id",      type = "string"},
		{name = "status",  type = "string"},
		{name = "lock",    type = "number", nullable=true},

		{name = "action", type = "string"},
		{name = "attr",   type = "*"},
	}
	box.schema.space.create('tasks', { if_not_exists = true, format = format })

	box.space.tasks:create_index('primary', { unique = true, parts = {'id'}, if_not_exists = true})
	box.space.tasks:create_index('xq', { unique = false, parts = { 'status', 'id' }, if_not_exists = true})
	box.space.tasks:create_index('lock', { unique = false, parts = { 'lock', 'status', 'id' }, if_not_exists = true})
-- end)

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../?.lua;' .. package.path
end

require 'xqueue' ( box.space.tasks, {
	debug = true;
	fields = {
		status = 'status';
		lock  = 'lock';
	};
	features = {
		id = 'uuid',
	};
	workers = 1;
	worker = function(task)
		print(require'fiber'.self().id(), "got task",task.id, require'yaml'.encode(box.space.tasks:select()))
	end;
} )


print(require'yaml'.encode(box.space.tasks:select()))
for i = 1, 10 do
	local t = box.space.tasks:put{ lock=1, action = "doit", attr = {} }
	print('inserted', t.id, t.status)
end

require'console'.start()
os.exit()
