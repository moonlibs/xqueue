---@diagnostic disable: inject-field
local xqueue = require 'xqueue'
require 'test.setup'
local clock  = require 'clock'
local fiber  = require 'fiber'

local t = require 'luatest' --[[@as luatest]]
local g = t.group('network')

local netbox = require 'net.box'

g.before_each(function()
	if box.space.queue then
		box.space.queue:truncate()
		for i = #box.space.queue.index, 0, -1 do
			local ind = box.space.queue.index[i]
			ind:drop()
		end
		box.space.queue:drop()
	end
end)

function g.test_untake()
	local queue = box.schema.space.create('queue', { if_not_exists = true }) --[[@as xqueue.space]]
	queue:format({
		{ name = 'id',      type = 'unsigned' },
		{ name = 'status',  type = 'string'   },
		{ name = 'runat',   type = 'number'   },
		{ name = 'payload', type = 'any'      },
	})

	queue:create_index('primary', { parts = {'id'} })
	queue:create_index('status', { parts = {'status', 'id'} })
	queue:create_index('runat', { parts = {'runat', 'id'} })

	xqueue.upgrade(queue, {
		features = {
			id = 'time64',
			delayed = true,
		},
		fields = {
			status = 'status',
			runat = 'runat',
		},
	})

	local tt = netbox.connect('127.0.0.1:3301', { wait_connected = true })
	t.assert(tt:ping(), "connected to self")

	local task = queue:put({ payload = { time = clock.time() } })
	t.assert(task, ":put() has been inserted task")

	local awaiter_fin = fiber.channel()

	fiber.create(function()
		local ret, is_processed = queue:wait(task, 3)
		t.assert_equals(ret.id, task.id, "Task has been awaited")
		t.assert_equals(is_processed, true, "Task has been processed by the consumer")
		t.assert(awaiter_fin:put({ ret, is_processed }), "awaiter results were measured")
	end)

	local taken = tt:call('box.space.queue:take', {1}, { timeout = 1 })
	t.assert(taken, ":take() returned task via the network")
	t.assert_equals(task.id, taken.id, "retutned the same task")

	-- untake:
	tt:close()
	fiber.sleep(1)

	t.assert_equals(queue:get({ task.id }).status, 'R', "task was returned to R status")

	tt = netbox.connect('127.0.0.1:3301', { wait_connected = true })

	taken = tt:call('box.space.queue:take', {1}, { timeout = 1 })
	t.assert(taken, ":take() returned task via the network (2nd)")
	t.assert_equals(task.id, taken.id, "retutned the same task (2nd)")

	local processed_at = clock.time()
	local acked = tt:call('box.space.queue:ack', {taken, { update = {{'=', 'payload', { processed_at = processed_at }}} }})
	t.assert_equals(acked.id, taken.id, ":ack() returned taken but completed task")

	local awaiter_res = awaiter_fin:get()
	t.assert_equals(awaiter_res[1].id, acked.id, "awaiter saw acknowledged task")
	t.assert_equals(awaiter_res[2], true, "awaiter saw task as processed")

	tt:close()
end
