box.cfg{
	listen = '127.0.0.1:4221'
}
box.once('access:v1', function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

local function newmap(arg)
	arg = arg or {}
	local mt = getmetatable(arg)
	if mt then
		mt.__serialize = 'map'
	else
		setmetatable(arg, { __serialize = 'map' })
	end
	return arg
end

require 'strict'.on()

local format = {
	{ name = "id",      type = "number"   }, -- taskid
	{ name = "status",  type = "string"   }, -- status
	{ name = "runat",   type = "number"   }, -- runat
	{ name = "payload", type = "map"      }, -- payload
}
local _, e = box.schema.create_space('myqueue', {
	format = format,
	if_not_exists = true
})

if e ~= 'created' then
	box.space.myqueue:format(format)
end

box.space.myqueue:create_index('primary', {
	parts = {"id"},
	if_not_exists = true,
})
box.space.myqueue:create_index('xq', {
	parts = {"status", "id"},
	if_not_exists = true,
})
box.space.myqueue:create_index('run', {
	parts = {"runat","id"},
	if_not_exists = true,
})

if not package.path:match('%.%./%?%.lua;') then
	package.path = '../?.lua;' .. package.path
end

require 'xqueue'.upgrade(box.space.myqueue, {
	fields = {
		status = 'status';
		runat = 'runat'
	};
	features = {
		id = "time64",
		delayed = true,
		ttr = 10,
		ttl = true,

		retval = 'tuple',
	};
	debug = true
})

local test = require 'tap'.test("putwait")
local fiber = require 'fiber'

local function worker()
	-- worker:
	fiber.yield()

	local task = box.space.myqueue:take(1)
	test:ok(task, "task was taken #1")

	box.space.myqueue:release(task, {
		update = {
			{ '=', 4, newmap{ was_taken = true } },
		}
	})

	-- take task back
	task = box.space.myqueue:take(1)
	test:ok(task, "task was taken #2")

	box.space.myqueue:update({ task.id }, {
		{ '=', 4, newmap{ processed = true } },
	})

	-- Yeah! We've managed to process it:
	box.space.myqueue:ack(task)
end

do
	-- consumer:
	fiber.create(worker)
	-- producer:
	local task, processed = box.space.myqueue:put({
		id = 1, payload = { initiated = true }
	}, { wait = 3 })

	test:ok(task, "task was returned for producer")
	test:ok(processed, "task was processed")
	test:is(box.space.myqueue:len(), 0, "queue is empty")

	task, processed = box.space.myqueue:put({
		id = 2, payload = newmap(),
	}, { wait = 2, ttl = 1 })
	test:ok(task, "task was returned for producer")
	test:is(processed, false, "task wasn't processed because was killed by TTL")
	test:is(box.space.myqueue:len(), 0, "queue is empty")
end

do
	-- consumer:
	fiber.create(worker)
	-- producer:
	local task = box.space.myqueue:put({
		id = 3, payload = { initiated = true }
	})

	local processed
	task, processed = box.space.myqueue:wait(task, 3)
	test:ok(task, "task was returned for producer")
	test:ok(processed, "task was processed")

	test:is(box.space.myqueue:len(), 0, "queue is empty")
end


os.exit()