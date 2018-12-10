box.cfg{
	-- listen = '127.0.0.1:4221'
}
box.once('access:v1', function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

require 'strict'.on()

box.once('schema',function()
	local format = {
		{name = "id",      type = "unsigned"},
		{name = "status",  type = "string"},
	}

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../../?.lua;' .. package.path
end

local format = {
	{ name = "id",      type = "string" };
	{ name = "status",  type = "string" };
	{ name = "cnt",     type = "num"};
}
local spacer = require 'spacer'
spacer.silent = true
spacer.create_space('xqtask', format, {
	{ name = 'primary', type = 'tree', parts = {                    'id' }};
	{ name = 'xq',      type = 'tree', parts = {          'status', 'id' }};
})

local DEBUG = true
local workers = 1

local base58 = require 'base58'
local uuid   = require 'uuid'
local json   = require 'json'
require 'xqueue' ( box.space.xqtask, {
	debug  = DEBUG;
	format = format;
	fields = {
		status = 'status';
	};
	features = {
		-- id      = function() return "TSK"..base58.encode(uuid.new():bin()); end;
		id      = function() return "TSK"..base58.encode_base58(uuid.new():bin()); end;
		-- buried  = true;
		-- delayed = true;
		-- ttl     = true;
	};
	workers = workers;
	worker  = function(task)
		print(">>>worker", json.encode(task))
		if task.cnt < 3 then
			box.space.xqtask:bury(task, {
				update={
					{'=', 3, task.cnt + 1}
				},
			})
		end
	end;
})

for _ = 1, 3 do
	box.space.xqtask:put({cnt=0})
end

require'console'.start()
os.exit()

end)
