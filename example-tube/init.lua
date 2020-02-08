box.cfg{}

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

box.space.utube:truncate()

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

		retval  = 'tuple',
	},
})

local fiber = require 'fiber'
local test = require 'tap'.test("tubes")

for i = 1, 10 do
	local task = box.space.utube:put({
		tube    = "tube-" .. i,
		payload = {
			id   = i,
			ctime = require 'clock'.realtime64(),
		},
		nice = i,
	})
	test:ok(task, "task was returned by :put")
end

for i = 10, 1, -1 do
	local tube = 'tube-' .. i
	local task = box.space.utube:take({
		tube = tube,
	})

	test:ok(task, "task was taken from tube: " .. tube)
	test:is(task.tube, tube, "task is from the same tube consumer requested")

	local notask = box.space.utube:take({
		tube = tube,
		timeout = 0.5,
	})

	test:isnil(notask, "no task because tube " .. tube .. " is empty")

	if i % 2 == 0 then
		local ret = box.space.utube:ack(task)
		test:ok(ret, ":ack returned processed task")
		test:is(ret.id, task.id, ":ack returned same task with same task.id")
	else
		local ret = box.space.utube:release(task)
		test:ok(ret, ":release returned processed task")
		test:is(ret.id, task.id, ":release returned same task with same task.id")
		test:is(ret.status, 'R', ":release retutned task in status 'R'")
	end
end

do
	-- common worker:
	for i = 1, box.space.utube:len() do
		local task = box.space.utube:take(0)
		test:ok(task, ":take returned task from queue")
		local tube = 'tube-' .. (2*i-1)
		test:is(task.tube, tube, "task must be from tube: " .. tube)

		local ret = box.space.utube:release(task, { delay = 1 })
		test:is(ret.id, task.id, ":release successfully returned task back to queue")
		test:is(ret.status, "W", "task was returned to status 'W' because was released with delay")
	end
end

-- clears queue:
box.space.utube:truncate()

-- Let's check how take+put works:
do
	local first = fiber.channel(1)
	local second = fiber.channel(1)

	local tubename = 'tube'

	fiber.create(function()
		-- creating this fiber we initiated new session
		-- so only this fiber takes ownership for this task
		second:put(box.space.utube:take(1), 0)
	end)

	fiber.create(function()
		first:put(box.space.utube:take({
			tube = tubename,
			timeout = 1,
		}), 0)
	end)

	fiber.sleep(0.1)

	for i = 1, 2 do
		box.space.utube:put({
			tube = tubename,
			payload = { id = i },
			nice = 512,
		})
	end

	local task1 = first:get(0.1)
	test:ok(task1, "tube consumer must receive task")
	test:is(task1.payload.id, 1, "tube consumer must receive task first")

	local task2 = second:get(0.1)
	test:ok(task2, "common consumer must receive task")
	test:is(task2.payload.id, 2, "common consumer must receive task second")
end

os.exit()