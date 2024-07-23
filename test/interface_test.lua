---@diagnostic disable: inject-field
local xqueue = require 'xqueue'
require 'test.setup'
local clock  = require 'clock'
local fun    = require 'fun'
local log    = require 'log'
local json   = require 'json'

local t = require 'luatest' --[[@as luatest]]
local g = t.group('interface')

---@return xqueue.space
local function setup_queue(opts)
	local space = box.schema.space.create(opts.name, {if_not_exists = true})
	space:format(opts.format)
	space:create_index('primary', { parts = {'id'} })
	if opts.fields.runat then
		space:create_index('runat', { parts = {'runat', 'id'} })
	end

	if opts.fields.priority then
		space:create_index('xq', { parts = {'status', tostring(opts.fields.priority), 'id'} })
	else
		space:create_index('xq', { parts = {'status', 'id'} })
	end

	if opts.fields.tube then
		if opts.fields.priority then
			space:create_index('tube', { parts = { 'tube', 'status', tostring(opts.fields.priority), 'id' } })
		else
			space:create_index('tube', { parts = { 'tube', 'status', 'id' } })
		end
	end

	xqueue.upgrade(space, opts)
	return space
end

local fiber = require 'fiber'
local netbox = require 'net.box'

function g.test_producer_consumer()
	local queue = setup_queue({
		name = 'simpleq',
		format = {
			{ name = 'id',      type = 'string' },
			{ name = 'status',  type = 'string' },
			{ name = 'payload', type = 'any'    },
		},
		features = {
			id = 'uuid',
			keep = false,
		},
		fields = {
			id = 'id',
			status = 'status',
		},
		retval = 'table',
	} --[[@as xqueue.upgrade.options]])

	local producer = fiber.create(function()
		fiber.self():set_joinable(true)
		fiber.yield()
		for _ = 1, 10 do
			queue:put({ payload = { id = math.random() } })
		end
		queue:put({ payload = { id = 0, is_last = true } })
	end)

	local consumer = fiber.create(function()
		fiber.self():set_joinable(true)
		fiber.yield()

		repeat
			-- taking loop
			local task
			while not task do
				task = queue:take({ timeout = 1 })
			end
			queue:ack(task)
		until task.payload.is_last

		-- consumer has seen last task.
		repeat
			local task = queue:take(0)
			if task then queue:ack(task) end
		until not task
	end)

	producer:join()
	consumer:join()

	t.assert_equals(queue:len(), 0, "Queue must be empty")
end


function g.test_producer_consumer_with_feedback()
	local format = {
		{ name = 'id',      type = 'unsigned' },
		{ name = 'nice',    type = 'unsigned' },
		{ name = 'status',  type = 'string'   },
		{ name = 'runat',   type = 'number'   },
		{ name = 'attempt', type = 'unsigned' },
		{ name = 'payload', type = 'any'      },
		{ name = 'context', type = 'any'      },
		{ name = 'error',   type = 'any'      },
	}
	local F = fun.iter(format):enumerate():map(function(no, f) return f.name, no end):tomap()

	local queue
	queue = setup_queue({
		name = 'q',
		format = format,
		features = {
			id = 'time64',
			buried = true,
			delayed = true,
			retval = 'table',
			zombie = 5,
		},
		debug = true,
		fields = {
			runat = 'runat',
			status = 'status',
			priority = 'nice',
		},
		workers = 2,
		worker = function(task)
			fiber.sleep(math.random()) -- random sleep
			if fiber.id() % 2 == task.id % 2 then
				queue:ack(task, {update = {
					{ '+', F.attempt, 1 },
					{ '=', F.context, { processed_by = fiber.id(), processed_time = fiber.time() } },
				}})
			else
				queue:release(task, {update = {
					{ '+', F.attempt, 1 },
					{ '=', F.error, { processed_by = fiber.id(), processed_time = fiber.time() } },
				}})
			end
		end,
	} --[[@as xqueue.upgrade.options]])

	local fibs = {}
	for i = 1, 10 do
		fibs[i] = fiber.create(function()
			fiber.self():set_joinable(true)
			fiber.yield()
			local res, is_processed = queue:put({ attempt = 0, nice = 0 }, { wait = 1 })
			log.info(":put() => %s %s", json.encode(res), is_processed)
			repeat
				res, is_processed = queue:wait(res, 0.1)
				log.info(":wait() => %s %s", json.encode(res), is_processed)
			until is_processed or res.status == 'D' or res.status == 'Z'
		end)
	end
	for i = 1, 10 do
		fibs[i]:join()
	end
end
