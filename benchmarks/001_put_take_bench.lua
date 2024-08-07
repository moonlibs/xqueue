local lb = require 'luabench'
local fiber = require 'fiber'
local log   = require 'log'
local fun   = require 'fun'

local xqueue = require 'xqueue'

box.cfg{ memtx_memory = 2^30, log_level = 4, checkpoint_count = 1, wal_max_size = 512*2^20 }
box.schema.space.create('queue', {
	if_not_exists = true,
	format = {
		{ name = 'id',     type = 'unsigned' },
		{ name = 'status', type = 'string'   },
	},
})
box.space.queue:create_index('primary', { parts = {'id'}, if_not_exists = true })
box.space.queue:create_index('xq', { parts = {'status', 'id'}, if_not_exists = true })

xqueue.upgrade(box.space.queue, {
	fields = {
		id = 'id',
		status = 'status',
	},
	features = {
		id = 'time64',
		keep = false,
		delayed = false,
		buried = false,
		retval = 'tuple',
		zombie = false,
		ttl = false,
		ttr = false,
	},
	debug = false,
	workers = 32,
	worker = function(task)
		-- pass (auto-ack)
	end
})

local queue = box.space.queue --[[@as xqueue.space]]
lb.before_all(function() queue:truncate() end)
lb.after_all(function() box.space.queue:truncate() box.snapshot() end)

local M = {}

local function new_producer_bench_f(num)
	local start = fiber.cond()
	local limit = 0
	local produced = 0

	local done = fiber.channel()

	for _ = 1, num do
		fiber.create(function()
			while true do
				start:wait()

				local empty_tab = {}
				while produced < limit do
					produced = produced + 1
					local ok, err = pcall(queue.put, queue, empty_tab)
					if not ok then
						log.error(":put() => %s", err)
					end
				end

				-- await everything
				while queue:len() > 0 do
					for _, task in queue.index.xq:pairs() do
						queue:wait(task, 0.1)
					end
				end

				done:put(true)
			end
		end)
	end

	---@param sb luabench.B
	return function(sb)
		limit = sb.N
		produced = 0
		start:broadcast()

		for _ = 1, num do
			done:get() -- await all producers to finish
		end
	end
end

---@param b luabench.B
function M.bench_producer_await(b)
	b:run('producer-1', new_producer_bench_f(1))
	b:run('producer-2', new_producer_bench_f(2))
	b:run('producer-4', new_producer_bench_f(4))
	b:run('producer-8', new_producer_bench_f(8))
	b:run('producer-12', new_producer_bench_f(12))
	b:run('producer-16', new_producer_bench_f(16))
	b:run('producer-20', new_producer_bench_f(20))
	b:run('producer-24', new_producer_bench_f(24))
end

return M
