box.cfg{
	-- listen = '127.0.0.1:4221'
}
-- box.once('access:v1', function()
--         box.schema.user.grant('guest', 'read,write,execute', 'universe')
-- end)

require 'strict'.on()

-- box.once('schema',function()
	local format = {
		{name = "id",      type = "unsigned"},
		{name = "status",  type = "string"},
		{name = "runat",   type = "number"},
		{name = "attempt", type = "number"},

		{name = "action", type = "string"},
		{name = "attr",   type = "*"},
	}
	box.schema.space.create('tasks', { if_not_exists = true })

	box.space.tasks:create_index('primary', { unique = true, parts = {1, 'unsigned'}, if_not_exists = true})
	box.space.tasks:create_index('xq', { unique = false, parts = { 2, 'str', 1, 'unsigned' }, if_not_exists = true})
	box.space.tasks:create_index('run', { unique = true, parts = { 3, 'number', 1, 'unsigned' }, if_not_exists = true})
-- end)

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../?.lua;' .. package.path
end

require 'xqueue' ( box.space.tasks, {
	debug = true;
	format = format;
	fields = {
		status = 'status';
		runat  = 'runat';
	};
	features = {
		id = 'time64',
		delayed = true,
	};
	workers = 1;
	worker = function(task)
		print("got task",task)
	end;
} )


require'console'.start()
os.exit()
