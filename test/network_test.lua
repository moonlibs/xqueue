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
	local F = { id = 1, status = 2, runat = 3, payload = 4 }

	queue:create_index('primary', { parts = {'id'} })
	queue:create_index('status', { parts = {'status', 'id'} })
	queue:create_index('runat', { parts = {'runat', 'id'} })

	xqueue.upgrade(queue, {
		features = {
			id = 'time64',
			delayed = true,
			retval = 'table',
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
	local acked = tt:call('box.space.queue:ack', {taken, { update = {{'=', F.payload, { processed_at = processed_at }}} }}, {timeout = 1})
	t.assert_equals(acked[1], taken.id, ":ack() returned taken but completed task")

	local awaiter_res = awaiter_fin:get()
	t.assert_equals(awaiter_res[1].id, acked[1], "awaiter saw acknowledged task")
	t.assert_equals(awaiter_res[2], true, "awaiter saw task as processed")

	tt:close()
end

function g.test_not_untake_with_not_check_session()
	local queue = box.schema.space.create('queue', { if_not_exists = true }) --[[@as xqueue.space]]
	queue:format({
		{ name = 'id',      type = 'unsigned' },
		{ name = 'status',  type = 'string'   },
		{ name = 'runat',   type = 'number'   },
		{ name = 'payload', type = 'any'      },
	})
	local F = { id = 1, status = 2, runat = 3, payload = 4 }

	queue:create_index('primary', { parts = {'id'} })
	queue:create_index('status', { parts = {'status', 'id'} })
	queue:create_index('runat', { parts = {'runat', 'id'} })

	xqueue.upgrade(queue, {
		features = {
			id = 'time64',
			delayed = true,
			retval = 'table',
			not_check_session = true,
			ttr = 60,
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
	t.assert(box.space.queue.xq.taken, nil, "taken table is empty")
	t.assert(box.space.queue.xq.bysid, nil, "bysid table is empty")

	-- not untake on close connection
	tt:close()
	fiber.sleep(1)

	t.assert_equals(queue:get({ task.id }).status, 'T', "task still in T status")

	tt = netbox.connect('127.0.0.1:3301', { wait_connected = true })

	local processed_at = clock.time()
	local acked = tt:call('box.space.queue:ack', {taken, { update = {{'=', F.payload, { processed_at = processed_at }}} }}, {timeout = 1})
	t.assert_equals(acked[1], taken.id, ":ack() returned taken but completed task")

	local awaiter_res = awaiter_fin:get()
	t.assert_equals(awaiter_res[1].id, acked[1], "awaiter saw acknowledged task")
	t.assert_equals(awaiter_res[2], true, "awaiter saw task as processed")

	tt:close()
end

function g.test_untake_with_not_check_session_by_ttr()
	local queue = box.schema.space.create('queue', { if_not_exists = true }) --[[@as xqueue.space]]
	queue:format({
		{ name = 'id',      type = 'unsigned' },
		{ name = 'status',  type = 'string'   },
		{ name = 'runat',   type = 'number'   },
		{ name = 'payload', type = 'any'      },
	})
	local F = { id = 1, status = 2, runat = 3, payload = 4 }

	queue:create_index('primary', { parts = {'id'} })
	queue:create_index('status', { parts = {'status', 'id'} })
	queue:create_index('runat', { parts = {'runat', 'id'} })

	xqueue.upgrade(queue, {
		features = {
			id = 'time64',
			delayed = true,
			retval = 'table',
			not_check_session = true,
			ttr = 5,
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

	local taken = tt:call('box.space.queue:take', {1}, { timeout = 1 })
	t.assert(taken, ":take() returned task via the network")
	t.assert_equals(task.id, taken.id, "retutned the same task")
	t.assert(queue.xq.taken, nil, "taken table is empty")
	t.assert(queue.xq.bysid, nil, "bysid table is empty")

	fiber.create(function()
		local ret, is_processed = queue:wait(task, 7)
		t.assert_equals(ret.id, task.id, "Task has been awaited")
		t.assert_equals(is_processed, true, "Task has been processed by the consumer")
		t.assert(awaiter_fin:put({ ret, is_processed }), "awaiter results were measured")
	end)

	-- not untake on close connection
	tt:close()
	t.assert_equals(queue:get({ task.id }).status, 'T', "task still in T status")

	-- untake by ttr
	fiber.sleep(5)
	t.assert_equals(queue:get({ task.id }).status, 'R', "task was returned to R status")

	tt = netbox.connect('127.0.0.1:3301', { wait_connected = true })

	taken = tt:call('box.space.queue:take', {1}, { timeout = 1 })
	t.assert(taken, ":take() returned task via the network (2nd)")
	t.assert_equals(task.id, taken.id, "retutned the same task (2nd)")
	t.assert(queue.xq.taken, nil, "taken table is empty")
	t.assert(queue.xq.bysid, nil, "bysid table is empty")

	local processed_at = clock.time()
	local acked = tt:call('box.space.queue:ack', {taken, { update = {{'=', F.payload, { processed_at = processed_at }}} }}, {timeout = 1})
	t.assert_equals(acked[1], taken.id, ":ack() returned taken but completed task")

	local awaiter_res = awaiter_fin:get()
	t.assert_equals(awaiter_res[1].id, acked[1], "awaiter saw acknowledged task")
	t.assert_equals(awaiter_res[2], true, "awaiter saw task as processed")

	tt:close()
end
