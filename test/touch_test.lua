---@diagnostic disable: inject-field
local fiber = require 'fiber'
local clock = require 'clock'
local xqueue = require 'xqueue'

require 'test.setup'

local t = require 'luatest' --[[@as luatest]]
local g = t.group('touch')

local queue
local F
local default_time_to = 0.25

g.before_each(function()
	if box.space.queue then
		box.space.queue:truncate()
		for i = #box.space.queue.index, 0, -1 do
			local ind = box.space.queue.index[i]
			ind:drop()
		end
		box.space.queue:drop()
	end

    queue = box.schema.space.create('queue', { if_not_exists = true }) --[[@as xqueue.space]]
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

	F = { id = 1, status = 2, runat = 3, payload = 4 }

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
			keep = true,
            ttr = default_time_to,
            ttl = default_time_to,
		},
	})
end)

function g.test_touch_ttr()
    local puted_task = queue:put({payload = { 'x' }}) --[[@as test.xqueue.delayed.tuple]]
	t.assert_equals(puted_task.status, 'R', 'queue:put(...) must insert task in R status')

    local task = queue:take({timeout=0})
    t.assert_equals(task.id, puted_task.id, 'queue not empty')
    t.assert_le(task.runat, clock.realtime() + default_time_to, 'taked task must have runat le than now+ttr')

    -- honest worker
    local begin_at = clock.realtime()
    while clock.realtime() <= begin_at + default_time_to*4 do
        fiber.sleep(default_time_to - 0.01)
        queue:touch(task, {increment=default_time_to})
    end

    local acked_task = queue:ack(task)
    t.assert_equals(acked_task.status, 'D', 'task must have Done status')
end

function g.test_touch_ttl()
    local puted_task = queue:put({payload = { 'x' }}) --[[@as test.xqueue.delayed.tuple]]
	t.assert_equals(puted_task.status, 'R', 'queue:put(...) must insert task in R status')

    local task
    local begin_at = clock.realtime()
    while clock.realtime() <= begin_at + default_time_to*4 do
        fiber.sleep(default_time_to - 0.01)
        task = queue:touch(puted_task, {increment=default_time_to})
    end

    t.assert_equals(task.status, 'R')
    t.assert_gt(task.runat, puted_task.runat + default_time_to*4 - 0.01)
    fiber.sleep(default_time_to*2)
    t.assert_equals(queue:take(), nil, 'task must be removed by ttl')
end
