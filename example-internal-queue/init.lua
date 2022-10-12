#!/usr/bin/env tarantool
local log = require 'log'
local http = require 'http.client'
local fun = require 'fun'
local uri = require 'uri'

box.cfg { listen = 3301, memtx_memory = 2^31 }
box.schema.user.grant('guest', 'super', nil, nil, { if_not_exists = true })

require 'schema'

---@type Queue
local q = require 'queue' {
	take_timeout = 1,
	space = 'queue',

	zombie = true,
	zombie_delay = 3600,

	backoff = {
		delay_base = 1.5,
		max_delay = 15,
		max_attempt = 3,
	},

	workers = 0,
	tubes = {
		-- { tube = 'parse', workers = 4 },
		-- { tube = 'download', workers = 2 },
	},
}

function q:on_fail(err)

end

q:on_take('parse', function(task)
	local body = task.payload.body
	if type(body) ~= 'string' then
		task:bury("body is not a string")
		return false
	end

	if #body > 2^20 then
		q:ack(task, { processed = false }, { keep = false, delay = 1 })
		return false
	end
	return task
end)

q:handle('parse', function (task)
	log.info("Start processing %s", task)

	local body = task.payload.body
	---@cast body string

	---@type string[]
	local urls = {}
	for href in body:gmatch([[href="(https://[^"]+)"]]) do
		local u = uri.parse(href)
		if type(u) == 'table' and (u.scheme == 'http' or u.scheme == 'https') and u.host:match("wikipedia.org") then
			local link = uri.format(u)
			if link then
				table.insert(urls, link)
			end
		end
	end

	-- uniquealize urls:
	---@type string[]
	urls = fun.totable(fun.zip(urls, fun.ones()):tomap())

	log.info("From url %s discovered %d links", task.dedup, #urls)

	box.atomic(function()
		for _,link in pairs(urls) do
			q:put({ dedup=link, tube='download', kind='download', payload={url=link, timeout=3} }, {skip_if_exists = true})
		end
		task.payload.body = '...'
		task:ack({ processed_at = os.time(), links = urls }, { update = { payload = task.payload } })
	end)
end)

q:on_take('download', function(task)
	local url = task.payload.url
	if type(url) ~= 'string' then
		task:bury("malformed url")
		return false
	end

	if not url:match("wikipedia%.org") then
		task:bury("outside wikipedia.org")
		return false
	end
end)

q:handle('download', function(task)
	-- log.info("Start processing %s", task)

	local url = task.payload.url
	local timeout = task.payload.timeout
	---@cast url string
	---@cast timeout number

	local result = http.get(url, { timeout = timeout })
	if result.status ~= 200 then
		return task:backoff(("http not 200: %s:%s"):format(result.status, result.body:sub(1, 1024)))
	end

	if #result.body > 2^20 then
		return task:ack({ status = 200, processed_at = os.time(), reason = "Body is too long" })
	end

	box.atomic(function()
		q:put({ dedup = url, tube = 'parse', kind = 'parse', payload = { body = result.body } }, { skip_if_exists = true })
		task:ack({ status = 200, processed_at = os.time() })
	end)
end)

q:put({
	kind = 'download',
	tube = 'download',
	dedup = 'https://en.wikipedia.org/wiki/Comparison_sort',
	payload = { url = 'https://en.wikipedia.org/wiki/Comparison_sort', timeout = 10 },
}, {
	restart_if_exists = true,
})

rawset(_G, 'queue', q)
require 'console'.start()
os.exit(0)
