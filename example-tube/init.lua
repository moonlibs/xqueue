box.cfg{
	-- wal_mode = 'none',
}

require 'strict'.on()

local format = {
	{ name = "id",        type = "unsigned" }, -- primary ID of each task (box.time64())
	{ name = "tube",      type = "string"   }, -- name of the tube
	{ name = "status",    type = "string"   }, -- status of the task
	{ name = "runat",     type = "number"   }, -- runat
	{ name = "nice",      type = "unsigned" }, -- priority of the task - the lower the higher
	{ name = "payload",   type = "map"      }, -- some payload of the task
}

-- definately works for Tarantool 1.10.3-0-g0b7078a93 :
-- utube stands for unordered tubes
box.schema.create_space('utube', {
	format = format,
	temporary = true,
	if_not_exists = true,
})

box.space.utube:create_index('primary', {
	parts = {"id"},
	if_not_exists = true,
})
box.space.utube:create_index('xqtube', {
	parts = {"tube", "status", "nice", "id"},
	if_not_exists = true,
})
box.space.utube:create_index('xq', {
	parts = {"status", "nice", "id"},
	if_not_exists = true,
})
box.space.utube:create_index('run', {
	parts = {"runat","id"},
	if_not_exists = true,
})

do
	local fio = require 'fio'
	local root = fio.abspath(debug.getinfo(1).source:gsub("@","")):gsub("[^/]+$", "")
	package.path = root .. "../?.lua;" .. package.path
end

require 'xqueue'.upgrade(box.space.utube, {
	debug  = true,
	format = format,
	fields = {
		status   = 'status',
		runat    = 'runat',
		priority = 'nice',
		tube     = 'tube',
	},
	features = {
		id      = 'time64',
		delayed = true,
		buried  = true,
		zombie  = 60,

		retval  = 'tuple',
	},
})

for i = 1, 10 do
	box.space.utube:put({
		tube    = "tube-" .. i,
		payload = {
			id   = i,
			ctime = require 'clock'.realtime64(),
		},
		nice = i,
	})
end

for i = 10, 1, -1 do
	local tube = 'tube-' .. i
	local task = box.space.utube:take({
		tube = tube,
	})

	assert(task, "task not taken from tube: " .. tube)
	assert(task.tube == tube, "task must be taken from specific tube")

	local notask = box.space.utube:take({
		tube = tube,
		timeout = 0.5,
	})

	assert(not notask, "task was not taken because tube is empty")

	if i % 2 == 0 then
		box.space.utube:ack(task)
	else
		box.space.utube:release(task)
	end
end

require 'console'.start()
os.exit()