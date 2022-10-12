---@class Queue
---@field q xqueueSpace space with Queue
---@field opts QueueOptions options of the queue
local M = require 'obj'.class({}, 'queue')

local background = require 'background'
local xqueue = require 'xqueue'
local Task = require 'task'
local fun = require 'fun'
local fiber = require 'fiber'
local json = require 'json'
local log = require 'log'

local function traceback(err)
	local i = 2
	local trace = {}
	while true do
		local slot = debug.getinfo(i, "Snl")
		if not slot then break end

		local line = {
			"\t", slot.source:sub(2),
		}

		if slot.currentline > 0 then
			table.insert(line, (":%d:"):format(slot.currentline))
		end

		local namewhat_ch = slot.namewhat:sub(1,1)

		if namewhat_ch ~= "" then
			table.insert(line, (" in function %s"):format(slot.name))
		elseif namewhat_ch == "m" then
			table.insert(line, " in main chunk")
		elseif namewhat_ch == "C" or namewhat_ch == "t" then
			table.insert(line, " ?")
		else
			table.insert(line, (" in function %s:%d"):format(
				slot.source:sub(2), slot.linedefined
			))
		end

		trace[i-1] = table.concat(line)

		i = i + 1
	end
	return setmetatable({
		error = err,
		trace = table.concat(trace, "\n"),
	}, {
		__tostring = function(self)
			return ("%s\nstack traceback:\n%s"):format(self.error, self.trace)
		end
	})
end


---@class QueueBackoff
---@field max_delay number? (Default: 3600) maximal delay for the task
---@field max_attempt number? (Default: 100) maximal numbers of take attempts for the task
---@field delay_base number? (Default: 1.5) base of exponential delay

---@class QueueOptions
---@field take_timeout number timeout of take in seconds
---@field backoff QueueBackoff backoff options
---@field restart_after number delay in seconds to restart background worker on fatal failure
---@field process_timeout number? (Default: 60) timeout of single Task processing


function M:__serialize()
    local space = self.q
    return ("Queue <%s:%d:%.2fMb> %s"):format(
        self.q.name, space:len(), tonumber(space:bsize()) / 1024 / 1024,
		json.encode(self:stats().counts)
	)
end

---@class QueueConfiguration
---@field autostart boolean? runs background workers
---@field workers number? (Default: 1) sets amount of background workers
---@field backoff QueueBackoff backoff configuration
---@field keep boolean? (Default: false) enables Done status in xqueue
---@field zombie boolean? (Default: false) enables Zombie status in xqueue
---@field zombie_delay number? sets delay for Zombie tasks before extermination
---@field space string name of the xqueue space
---@field take_timeout number? (Default: 1) consumer's timeout during xqeueue:take() method
---@field tubes { tube: string, workers: number }[]? configuration of tube-related workers
---@field process_timeout number? (Default: 60) timeout of single Task processing

---Builder of the Queue
---@param cfg QueueConfiguration configuration of queue
---@return Queue
function M:_init(cfg)
    local space = assert(box.space[cfg.space], ("space %s does not exists"):format(cfg.space))

    xqueue.upgrade(space, {
		debug = true,
        fields = {
            status   = 'status',
            priority = 'nice',
            tube     = 'tube',
			runat    = 'runat',
        },
        features = {
            id = 'time64',
            retval = 'tuple',
            buried = true,
            delayed = true,
            keep = cfg.keep,

            zombie = cfg.zombie,
            zombie_delay = cfg.zombie_delay,

            ttl = false,
			ttr = false,
		},
    })

	cfg.backoff = cfg.backoff or {}

    self.opts = {}
    self.opts.take_timeout = cfg.take_timeout or 1
    self.opts.backoff = cfg.backoff
    self.opts.backoff.max_delay = cfg.backoff.max_delay or 3600
	self.opts.backoff.max_attempt = cfg.backoff.max_attempt or 100
    self.opts.backoff.delay_base = cfg.backoff.delay_base or 1.5
    self.opts.tubes = cfg.tubes or {}
    self.opts.autostart = cfg.autostart
	self.opts.process_timeout = cfg.process_timeout or 60

	---@type table<string, {job: any, task: Task, taken_at: number}>
	self._task_map = {}

	---@cast space xqueueSpace
    self.q = space

    self._on = {}
    self.handlers = {}

	for _, t in ipairs(self.opts.tubes) do
		self:spawn(t.workers, t.tube)
    end

	if cfg.workers then
		self:spawn(cfg.workers)
    end

    self.monitor = background {
        name = 'qmon',
        eternal = false,
        restart = 5,
        run_interval = 10,
        func = function(job) self:_check_workers(job) end,
		on_fail = function(_,err) self:on_fail(err) end,
	}

	return self
end

---Checks stucking workers
---@param _ any myjob
function M:_check_workers(_)
	---@type string[]
	local to_destroy = {}
	for task_key, taken_info in pairs(self._task_map) do
		if taken_info.taken_at + self.opts.process_timeout < fiber.time() then
			table.insert(to_destroy, task_key)
		end
    end

	for _, key in ipairs(to_destroy) do
        local job = self._task_map[key].job
		job:shutdown()
        job:shutdown()
        self.workers[job.worker_id] = self:_new_worker(job.take_tube)
		log.error("shutdown %s", job)
    end

	for _, key in ipairs(to_destroy) do
        local task = self._task_map[key].task
        task = task:fetch()
		if task.status == 'T' then
			local ok, err = pcall(function() task:backoff("qmon: autobackoff") end)
			if not ok then
				log.error("task wasn't backoffed: %s", err)
			end
		end
		self._task_map[key] = nil
    end
	log.info("qmon exec finished, restarted: %d", #to_destroy)
end

---@return Task[]
function M:top(status, n)
    return self.q.index.xq
        :pairs({ status })
		:take(n or 5)
        :map(function(t) return self:task(t) end)
		:totable()
end

---Gets task from space
---@param key any
---@return Task?
function M:get(key)
    local t = self.q:get(key)
	if not t then return end
	return self:task(t)
end

---Converts tuple to Task
---@param t box.tuple|table|Task?
---@return Task
function M:task(t)
	assert(t, "task is not given")
    if type(t) == 'cdata' then
        local tbl = t:tomap({ names_only = true })
		tbl._queue = self
        return Task(tbl)
    end

	t._queue = self
	return Task(t)
end

---Sets callback for take
---@param kind string type of the task
---@param func fun(task: Task): any
function M:on_take(kind, func)
	self._on[kind] = func
end

---Sets handler for kind of task
---@param kind string
---@param func fun(task: Task)
function M:handle(kind, func)
	self.handlers[kind] = func
end

---Takes task
---@param options { timeout: number?, tube: string? } take options
---@return Task?
function M:take(options)
	assert(tonumber(options.timeout), "timeout must be given")
    local tuple = self.q:take(options)
    if not tuple then return end
	local task = self:task(tuple)

	local on_take_hook = self._on[task.kind]

    if on_take_hook then
        local ret = on_take_hook(task)
        if ret == false then
            task = task:fetch()
            if task.status == 'T' then
				log.warn("Task %s wasnt released after on_take_hook returned false", task)
				return task:backoff("autobackoff: on_take_hook did not release task")
			end
			return
        end
		return ret or task
	end

	return task
end

function M:_new_worker(take_tube)
	return background {
		name = 'wrk:'..(take_tube or 'def')..'#' .. #self.workers+1,
		eternal = false,
		wait = 'rw',
		run_interval = 0,
		restart = self.opts.restart_after or 5,
		setup = function(job)
			job.take_tube = take_tube
		end,
		on_fail = function(_, err) log.error(err) return self:on_fail(err) end,
		func = function(job) return self:executor_f(job) end,
	}
end

function M:spawn(n, take_tube)
    self.workers = self.workers or {}

    for _ = 1, n or 1 do
        local job = self:_new_worker(take_tube)
        job.worker_id = #self.workers + 1
		self.workers[job.worker_id] = job
	end
end

function M:despawn(n, tube)
	if tube then
        local tube_workers = {}
		for _, w in pairs(self.workers) do
			if w and w.take_tube == tube then
				table.insert(tube_workers, w)
			end
        end
		for i = 1, math.min(n or 1, #tube_workers) do
            local w = tube_workers[i]
            w:shutdown()
			self.workers[w.worker_id] = nil
        end
    else
		local limit = n or 1
		for _, w in pairs(self.workers) do
			if limit == 0 then break end
			limit = limit - 1

			self.workers[w.worker_id] = nil
			w:shutdown()
		end
    end

	-- Rebuild self.workers (remove empty slots)
	local max_id = 0
	for id, _ in pairs(self.workers) do
		if max_id < id then max_id = id end
	end

    local new_workers = {}
	for _, worker in pairs(self.workers) do
        new_workers[#new_workers + 1] = worker
		worker.worker_id = #new_workers
    end

	self.workers = new_workers
end

---Main executor
---@param job any
function M:executor_f(job)
    local task = self:take({ timeout = self.opts.take_timeout or 1, tube = job.take_tube })
    if not task then return end
	-- log.warn("Staring processing task %s", task)
    job.task = task

	local key = tostring(task.id)
	self._task_map[key] = { job = job, task = task, taken_at = fiber.time() }

	local handler = self.handlers[task.kind]

    if not handler then
		self._task_map[key] = nil
		log.warn("Handler for %s not found", task.kind)
        task:backoff(("handler for %s not found"):format(task.kind))
		return
    end

    local ok, err = xpcall(handler, traceback, task)
    self._task_map[key] = nil
	job.task = nil

    if not ok then
        self:on_fail(err)

		local exists = self:get({task.id})
		if exists and exists.status == "T" then
			task:backoff(("handler did not release task: %s"):format(err))
			return
		end
    end

	local exists = self:get({task.id})
    if exists and exists.status == "T" then
		task:backoff("handler did not release task")
		return
	end
end

function M:on_fail(err)
	log.error("job failed: %s", err)
end

---@class QueuePutOptions
---@field skip_if_status xqStatus[]? Does not put if task already in status.
---@field skip_if_exists boolean? Does not put if already exists
---@field restart_if_exists boolean Restarts (without update) existing task (if it is not in RT statuses).
---@field delay number? seconds to delay task
---@field wait number? seconds to wait task to be processed

---Puts new task
---@param task Task|table
---@param options QueuePutOptions
---@return Task, string? reason
function M:put(task, options)
    options = options or {}

	---@type table<xqStatus, number>
    local skip_in_status = fun.zip(options.skip_if_status or {}, fun.ones()):tomap()

	if task.dedup == nil or task.kind == nil or task.payload == nil then
        box.error({ reason = "Fields {dedup, kind, payload} must be given" })
    end

    task.attempt = 0
    task.payload = setmetatable(task.payload, { __serialize = 'map' })
    task.result = setmetatable(task.result or {}, { __serialize = 'map' })
    task.error = setmetatable(task.error or {}, { __serialize = 'map' })
    task.nice = tonumber(task.nice) or 512
	task.tube = task.tube or 'default'

    local tuple = self.q.index.dedup_kind:get({ task.dedup, task.kind })
    if not tuple then
        local ret = self.q:put(task, { delay = options.delay, wait = options.wait })
		return self:task(ret)
	end

    local exists = assert(self:task(tuple))

	if options.skip_if_exists then
		return exists
	end

	if skip_in_status[exists.status] then
		return exists
	end

	if options.restart_if_exists then
		if exists.status ~= "R" and exists.status ~= "T" then
            return self:task(self.q:update({ exists.id }, {
                { '=', 'status', 'R' },
                { '=', 'attempt', 0 },
				{ '=', 'runat',   0 },
			}))
        end
		return exists
    end

	error(("Task already exists %s"):format(tuple))
end

---Restarts task if it is not in RT statues
---@param key any
---@return Task? task # returns task if found
function M:restart(key)
    local task = self:get(key)
	if not task then
		return
    end
	if task.status == "R" or task.status == "T" then
		return task
	end
    return self:task(self.q:update({ task.id }, {
		{ '=', 'status', 'R' },
        { '=', 'attempt', 0 },
		{ '=', 'runat',   0 },
	}))
end

---Backoff
---@param task Task
---@param reason? string
---@return Task
function M:backoff(task, reason)
	if task.attempt+1 > self.opts.backoff.max_attempt then
		return self:bury(task, ("Max Attempt Reached. last: %s"):format(reason or 'unknown reason'))
	end
    return self:task(self.q:release({ task.id }, {
        update = {
            { '+', 'attempt', 1 },
            { '=', 'mtime', fiber.time() },
			{ '=', 'error', { reason = reason or 'Unknown reason', last_error = fiber.time() } },
        },
		delay = math.min(self.opts.backoff.max_delay, self.opts.backoff.delay_base^task.attempt),
	}))
end

---Putbacks task
---@param task Task
---@param opts { delay: number?, update: update_operation[]?, reason: string? } release options
---@return Task
function M:release(task, opts)
	return self:task(self.q:release({ task.id }, opts))
end

---Finishes task execution
---@param task Task
---@param result table
---@param opts? { delay: number?, keep: boolean? } acknowledge options
---@return Task
function M:ack(task, result, opts)
    opts = opts or {}
    opts.update = opts.update or {}

    local update = {
		{ '+', 'attempt', 1 },
		{ '=', 'mtime', fiber.time() },
		{ '=', 'result', setmetatable(result or {}, {__serialize='map'}) },
	}
	for key, value in pairs(opts.update) do
		table.insert(update, { '=', key, value })
    end

    return self:task(self.q:ack({ task.id }, {
        update = update,
		delay = opts.delay,
		keep = opts.keep,
	}))
end


---Stops task processing (and save it forever)
---@param task Task
---@param reason string Reason of Failure
---@return Task
function M:bury(task, reason)
	log.warn("Bury %s %s", task, reason)
    self.q:bury({ task.id }, {
        update = {
            { '+', 'attempt', 1 },
            { '=', 'mtime', fiber.time() },
			{ '=', 'error', { reason = reason or 'Unknown reason', last_error = fiber.time() } },
        },
    })
	return task:fetch()
end

---Returns xqueue stats
---@param pretty boolean?
---@return { counts: table<string,number>, transition: table }
function M:stats(pretty)
	return self.q:stats(pretty)
end

return M
