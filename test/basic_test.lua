---@diagnostic disable: inject-field
local fiber = require 'fiber'
local xqueue = require 'xqueue'

require 'test.setup'

local t = require 'luatest' --[[@as luatest]]
local g = t.group('basic')

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

function g.test_basic_queue()
	box.schema.space.create('queue', { if_not_exists = true })
	---@class test.xqueue.basic.tuple: box.tuple
	---@field id string
	---@field status string
	---@field payload any
	box.space.queue:format({
		{ name = 'id',      type = 'string' },
		{ name = 'status',  type = 'string' },
		{ name = 'payload', type = 'any' },
	})

	box.space.queue:create_index('primary', { parts = {'id'} })
	box.space.queue:create_index('status', { parts = {'status', 'id'} })

	xqueue.upgrade(box.space.queue, {
		debug = true,
		fields = {
			status = 'status',
		},
		features = {
			id = 'uuid',
		},
	})

	local task = box.space.queue:put({ payload = { task = 1 } }) --[[@as test.xqueue.basic.tuple]]
	t.assert_equals(task.status, 'R', "newly inserted task must be Ready")
	t.assert_items_include(task.payload, { task = 1 }, "payload of the task remains the same")

	local ft = fiber.time()
	local taken = box.space.queue:take()
	local elapsed = fiber.time()-ft

	t.assert(taken, "queue:take() must return task")
	t.assert_le(elapsed, 0.1, "queue:take() must return task after single yield")
	t.assert_equals(taken.status, 'T', "taken task must be in status T")

	local tuple = box.space.queue:get(taken.id) --[[@as test.xqueue.basic.tuple]]
	t.assert_equals(tuple.status, 'T', "status of taken task in space must be T")

	ft = fiber.time()
	local not_taken = box.space.queue:take(0.001)
	elapsed = fiber.time()-ft
	t.assert_not(not_taken, "second queue:take() must return nothing")
	t.assert_ge(elapsed, 0.001, "second queue:take() must be returned ≥ 0.001 seconds")
	t.assert_le(elapsed, 0.1, "second queue:take() must be returned in ≤ 0.1 seconds")

	box.space.queue:release(taken)
	tuple = box.space.queue:get(taken.id) --[[@as test.xqueue.basic.tuple]]

	t.assert_equals(tuple.status, 'R', "queue:release() must return task in R status")
	taken = box.space.queue:take(0.001)
	t.assert_equals(taken.status, 'T', "queue:take() after queue:release() must return task back")

	taken = box.space.queue:ack(taken)

	local nothing = box.space.queue:get(taken.id)
	t.assert_not(nothing, "queue:ack() for taken task must remove task from space")

	local taken_tasks = {}
	local fin = fiber.channel()

	local function taker(id)
		fiber.name('taker/'..id)
		local taken = box.space.queue:take({ timeout = 1 })
		if not taken then fin:put({ id = id }) return end

		local taken_id = taken.id
		t.assert_not(taken_tasks[taken_id], "task must be taken only once")
		taken_tasks[taken_id] = true

		box.space.queue:bury({ id = taken_id })
		t.assert_is(box.space.queue:get({taken_id}).status, 'B', "queue:bury() must move tuple to B")
		fin:put({id = id, task = taken_id})
	end

	fiber.create(taker, 1)
	fiber.create(taker, 2)

	local published = box.space.queue:put({})

	for _ = 1, 2 do
		local v = fin:get()
		if v.id == 1 then
			t.assert_equals(v.task, published.id)
		else
			t.assert_not(v.task)
		end
	end
	fin:close()

	t.assert_equals(box.space.queue:get(published.id).status, 'B', "task left in B status")

	nothing = box.space.queue:take({ timeout = 0.001 })
	t.assert_not(nothing, "task in B status is not takeable")

	box.space.queue:kick(published)
	t.assert_equals(box.space.queue:get(published.id).status, 'R', "queue:kick() kicks B task to R")

	taken = box.space.queue:take(0.001)
	t.assert_equals(taken.id, published.id, "published task can be retaken from R (after kick)")

	box.space.queue:ack({ id = taken.id })
	t.assert_not(box.space.queue:get(published.id), "queue:ack({id=id}) removes task from space")
end

function g.test_delayed_queue()
	local queue = box.schema.space.create('queue', { if_not_exists = true }) --[[@as xqueue.space]]
	---@class test.xqueue.delayed.tuple: box.tuple
	---@field id number
	---@field status string
	---@field runat number
	---@field payload any
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
		debug = true,
		fields = {
			runat  = 'runat',
			status = 'status',
		},
		features = {
			id = 'time64',
			delayed = true,
			zombie = true,
		},
	})

	local task_put_delay_500ms = queue:put({ payload = { id = 2 } }, {
		delay = 0.1,
	}) --[[@as test.xqueue.delayed.tuple]]
	t.assert_equals(task_put_delay_500ms.status, 'W', 'queue:put(...,{delay=<>}) must insert task in W status')
	t.assert_equals(queue:get({task_put_delay_500ms.id}).status, 'W', 'double check task status in space (must be W)')

	local task_put = queue:put({ payload = { id = 1 } }) --[[@as test.xqueue.delayed.tuple]]
	t.assert_equals(task_put.status, 'R', "queue:put() without delay in delayed queue must set task to R")
	t.assert_equals(queue:get({task_put.id}).status, 'R', 'double check task status in space (must be R)')

	t.assert_lt(task_put_delay_500ms.id, task_put.id, "first task must have smaller id than second (fifo constraint)")

	local taken = queue:take({ timeout = 0.05 })
	t.assert(taken, "task must be taken")
	---@cast taken -nil
	t.assert_equals(taken.id, task_put.id, 'queue:take() must return ready task')

	queue:release(taken, { delay = 0.12 })
	t.assert_equals(queue:get({taken.id}).status, 'W', 'queue:release(..., {delay=<>}) must put task in W')
	t.assert_le(queue:get({task_put_delay_500ms.id}).runat, queue:get({taken.id}).runat, "first task must wakeup earlier")

	local taken_delayed = queue:take({ timeout = 3 })
	t.assert(taken_delayed, 'delayed task must be taken after timeout')

	local taken_put = queue:take({ timeout = 3 })
	t.assert(taken_put, 'released task must be taken after timeout')

	t.assert_equals(taken_delayed.id, task_put_delay_500ms.id, "firstly delayed task must be taken")
	t.assert_equals(taken_put.id, task_put.id, "secondly released task must be taken")

	queue:ack(taken_delayed)
	queue:ack(taken_put)

	t.assert_is(queue:get(task_put.id).status, 'Z', "acknowledged task in zombied queue must left in Z status")
	t.assert_is(queue:get(taken_delayed.id).status, 'Z', "acknowledged task in zombied queue must left in Z status")

	-- put /take / release / take / ack+delay
	local put = queue:put({payload = { task = 2 }})
	local take = queue:take(0.001)

	t.assert_equals(put.id, take.id, "put/take returns same task on empty space")

	queue:release(take, {
		update = {
			{ '=', F.payload, { new = 2 } }
		}
	})

	t.assert_items_equals(queue:get(take.id).payload, { new = 2 }, ":release()/update must change tuple in space")
	t.assert_is(queue:get(take.id).status, 'R', ':release()/update without delay must return task into R')

	take = queue:take(0.001)

	queue:ack(take, {
		update = {
			{ '=', F.payload, { finished = 2 } },
		},
		delay = 0.5,
	})

	t.assert_is(queue:get(take.id).status, 'Z', ':ack()+delay return task into Z status')
	t.assert_items_equals(queue:get(take.id).payload, { finished = 2 }, ':ack()/update must replace tuple content')

	local nothing = queue:take({ timeout = 1.5 })
	t.assert_not(nothing, "Z tasks must are not takable")

	t.assert_not(queue:get(take.id), "Z task must be collected after delay time out exhausted")
end
