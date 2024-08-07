local lb = require 'luabench'
local fiber = require 'fiber'
local fun   = require 'fun'

local M = {}

local STATUS = { 'R', 'T', 'W', 'B', 'Z', 'D' }

lb.before_all(function()
	box.cfg{ memtx_memory = 2^30 }

	box.schema.space.create('q1', {
		if_not_exists = true,
		format = {
			{ name = 'id',     type = 'unsigned' },
			{ name = 'status', type = 'string' },
		},
	})

	box.space.q1:create_index('primary', { parts = {'id'}, if_not_exists = true })
	box.space.q1:create_index('xq', { parts = {'status', 'id'}, if_not_exists = true })

	if fiber.extend_slice then
		fiber.extend_slice({ err = 3600, warn = 3600 })
	end

	box.begin()
		local tab = {}
		for no = 1, 4e6 do
			tab[1], tab[2] = no, STATUS[math.random(#STATUS)]
			box.space.q1:replace(tab)
		end
	box.commit()
end)

lb.after_all(function()
	box.space.q1:drop()
	box.snapshot()
end)

function M.bench_iterate_all(b)
	local limit = b.N
	local scanned = 0
	local stats = {}
	for _, t in box.space.q1:pairs({}, { iterator = "ALL" }) do
		stats[t.status] = (stats[t.status] or 0ULL) + 1
		scanned = scanned + 1
		if limit == scanned then break end
	end
	b.N = scanned
end

function M.bench_count(b)
	local total = 0
	for _, s in pairs(STATUS) do
		total = total + box.space.q1.index.xq:count(s)
		if b.N < total then break end
	end
	b.N = total
end

return M
