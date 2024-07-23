---@diagnostic disable: inject-field
local xqueue = require 'xqueue'
require 'test.setup'

local t = require 'luatest' --[[@as luatest]]
local g = t.group('tube')

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

function g.test_tube_queue()
	local queue = box.schema.space.create('queue', { if_not_exists = true }) --[[@as xqueue.space]]
	queue:format({
		{ name = 'id',      type = 'unsigned' },
		{ name = 'tube',    type = 'string'   },
		{ name = 'status',  type = 'string'   },
		{ name = 'runat',   type = 'number'   },
		{ name = 'payload', type = 'any'      },
	})

	queue:create_index('primary', { parts = {'id'} })
	queue:create_index('tube_status', { parts = {'tube', 'status', 'id'} })
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
			tube = 'tube',
		},
	})

	local trans = setmetatable({ __cache = {}, },{__index=function(self, tx)
		local s,e = unpack(tostring(tx):split("_"))
		local n = s:upper().."-"..e:upper()
		self[n] = rawget(self, n) or 0
		local func = self.__cache[n] or function(inc) self[n] = self[n] + inc end
		self.__cache[n] = func
		return func
	end})

	for i = 1, 10 do
		local tube_name = 'tube-'..i
		local task = queue:put({ tube = tube_name, payload = { id = i } })
		t.assert_equals(task.payload.id, i, "task:put("..i..")")
		t.assert_items_include(task.payload, { id = i }, "payload of the task remains the same")
	end
	trans.x_r(10)

	for i = 10, 1, -1 do
		local tube_name = 'tube-'..i
		local task = queue:take({ tube = tube_name })
		trans.r_t(1)
		t.assert_equals(task.status, 'T', "task has been taken from "..tube_name)
		t.assert_equals(task.tube, tube_name, "task was returned from the tube we requested")

		local notask = queue:take({
			tube = tube_name,
			timeout = 0.005,
		})

		t.assert_equals(notask, nil, "no task should be returned from the empty tube")

		if i % 2 == 0 then
			local ret = queue:ack(task)
			trans.t_x(1)
			t.assert_is_not(ret, nil, ":ack() returns task back")
			t.assert_equals(task.id, ret.id, ":ack() returned same task")
		else
			local ret = queue:release(task)
			trans.t_r(1)
			t.assert_is_not(ret, nil, ":release() returns task back")
			t.assert_equals(task.id, ret.id, ":release() returned same task")
			t.assert_equals(ret.status, "R", ":release() returns task into Ready status")
		end
	end

	-- now test generic worker
	for i = 1, queue:len() do
		local task = queue:take(0) -- no-yield take
		t.assert_is_not(task, nil, ":take() returned task")
		---@cast task -nil

		trans.r_t(1)

		-- FIFO order of the tubes.
		local expected_tube_name = 'tube-'..(2*i-1)
		t.assert_equals(task.tube, expected_tube_name, ":take() must follow FIFO and return task from correct tube")

		local ret = queue:release(task, { delay = 1 })
		trans.t_w(1)
		t.assert_equals(ret.status, 'W', ":release(+delay) returns task into W state")
		t.assert_equals(ret.id, task.id, ":release() returned the same task we sent to release")
	end

	local stats = queue:stats()

	trans.__cache = nil
	setmetatable(trans, nil)
	t.assert_covers(stats, {
		counts = {
			B = 0,
			D = 0,
			R = 0,
			T = 0,
			W = 5,
			Z = 0,
		},
		transition = trans,
		tube = {},
	}, "queue:stats() with tubes")
end
