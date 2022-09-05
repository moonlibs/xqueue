local M = {}

local ffi = require 'ffi'
local log = require 'log'
local fiber = require 'fiber'
local clock = require 'clock'

local tuple_ctype = ffi.typeof(box.tuple.new())

local monotonic_max_age = 10*365*86400;

local function table_clear(t)
	if type(t) ~= 'table' then
		error("bad argument #1 to 'clear' (table expected, got "..(t ~= nil and type(t) or 'no value')..")",2)
	end
	local count = #t
	for i=0, count do t[i]=nil end
	return
end

local function is_array(t)
	local gen,param,state = ipairs(t)
	-- print(gen,param,state)
	local v = gen(param,state)
	-- print(v)
	return v ~= nil
end

local peers = {}

rawset(_G,"\0xq.on_connect",box.session.on_connect(function()
	local sid = box.session.id()
	local peer = box.session.peer()
	box.session.storage.peer = box.session.peer()
	peers[ sid ] = peer
	log.info("connected %s, sid=%s, fid=%s", peer, sid, fiber.id() )
end,rawget(_G,"\0xq.on_connect")))

rawset(_G,"\0xq.on_disconnect",box.session.on_disconnect(function()
	local sid = box.session.id()
	local peer = peers[ sid ]
	peers[ sid ] = nil
	log.info("disconnected %s, sid=%s, fid=%s", peer, sid, fiber.id() )
end,rawget(_G,"\0xq.on_disconnect")))


--[[

Field sets:
1. id, status (minimal required configuration)
2. id, status, priority
3. id, status, runat
4. id, status, priority, runat

Primary index variants:
1. index(status, [ id ])
2. index(status, priority, [ id ])


Format:
local format = box.space._space.index.name:get(space_name)[ 7 ]

Status:
	R - ready - task is ready to be taken
		created by put without delay
		turned on by release/kick without delay
		turned on from W when delay passed
		deleted after ttl if ttl is enabled

	T - taken - task is taken by consumer. may not be taken by other.
		turned into R after ttr if ttr is enabled

	W - waiting - task is not ready to be taken. waiting for its `delay`
		requires `runat`
		`delay` may be set during put, release, kick
		turned into R after delay
	
	B - buried - task was temporary discarded from queue by consumer
		may be revived using kick by administrator
		use it in unpredicted conditions, when man intervention is required
		Without mandatory monitoring (stats.buried) usage of buried is useless and awry

	Z - zombie - task was processed and ack'ed and *temporary* kept for delay

	D - done - task was processed and ack'ed and permanently left in database
		enabled when keep feature is set

	L - locked - task is locked via `lock' index. Tasks will be processed only 
		if there are no older tasks by `lock` index
	
	X - reserved for statistics

(TODO: reload/upgrade and feature switch)

Interface:

# Creator methods:

	M.upgrade(space, {
		format = {
			-- space format. applied to space.format() if passed
		},
		fields = {
			-- id is always taken from pk
			status   = 'status_field_name'    | status_field_no,
			runat    = 'runat_field_name'     | runat_field_no,
			priority = 'priority_field_name'  | priority_field_no,
			lock     = 'lock_field_name'      | lock_field_no,
		},
		features = {
			id = 'auto_increment' | 'uuid' | 'required' | function
				-- auto_increment - if pk is number, then use it for auto_increment
				-- uuid - if pk is string, then use uuid for id
				-- required - primary key MUST be present in tuple during put
				-- function - funciton will be called to aquire id for task

			buried = true,           -- if true, support bury/kick
			delayed = true,          -- if true, support delayed tasks, requires `runat`

			keep = true,             -- if true, keep ack'ed tasks in [D]one state, instead of deleting
				-- mutually exclusive with zombie

			zombie = true|number,    -- requires `runat` field
				-- if number, then with default zombie delay, otherwise only if set delay during ack
				-- mutually exclusive with keep

			ttl    = true|number,    -- requires `runat` field
				-- if number, then with default ttl, otherwise only if set during put/release
			ttr    = true|number,    -- requires `runat` field
				-- if number, then with default ttl, otherwise only if set
		},
	})

Producer methods:
	sp:put({...}, [ attr ]) (array or table (if have format) or tuple)

Consumer methods:

* id:
	- array of keyfields of pk
	- table of named keyields
	- tuple

sp:take(timeout) -> tuple
sp:ack(id, [ attr ])
	attr.update (only if support zombie)
sp:release(id, [ attr ])
	attr.ttr (only if support ttr)
	attr.ttl (only if support ttl)
	attr.update
sp:bury(id, [ attr ])
	attr.ttl (only if support ttl)
	attr.update

Admin methods:

sp:queue_stats()
sp:kick(N | id, [attr]) -- put buried task id or N oldest buried tasks to [R]eady


]]

local json = require 'json'
json.cfg{ encode_invalid_as_nil = true }
local yaml = require 'yaml'
local function dd(x)
	print(yaml.encode(x))
end


local function typeeq(src, ref)
	if ref == 'str' then
		return src == 'STR' or src == 'str' or src == 'string'
	elseif ref == 'num' then
		return src == 'NUM' or src == 'num' or src == 'number' or src == 'unsigned'
	else
		return src == ref
	end
end

local function is_eq(a, b, depth)
	local t = type(a)
	if t ~= type(b) then return false end
	if t == 'table' then
		for k,v in pairs(a) do
			if b[k] == nil then return end
			if not is_eq(v,b[k]) then return false end
		end
		for k,v in pairs(b) do
			if a[k] == nil then return end
			if not is_eq(v,a[k]) then return false end
		end
		return true
	elseif t == 'string' or t == 'number' then
		return a == b
	elseif t == 'cdata' then
		return ffi.typeof(a) == ffi.typeof(b) and a == b
	else
		error("Wrong types for equality", depth or 2)
	end
end


local function _tuple2table ( qformat )
	local rows = {}
	for k,v in ipairs(qformat) do
		table.insert(rows,"\t['"..v.name.."'] = t["..tostring(k).."];\n")
	end
	local fun = "return function(t,...) "..
		"if select('#',...) > 0 then error('excess args',2) end "..
		"return t and {\n"..table.concat(rows, "").."} or nil end\n"
	return dostring(fun)
end

local function _table2tuple ( qformat )
	local rows = {}
	for _,v in ipairs(qformat) do
		table.insert(rows,"\tt['"..v.name.."'] == nil and NULL or t['"..v.name.."'];\n")
	end
	local fun = "local NULL = require'msgpack'.NULL return function(t) return "..
		"t and box.tuple.new({\n"..table.concat(rows, "").."}) or nil end\n"
	-- print(fun)
	return dostring(fun)
end

local methods = {}

function M.upgrade(space,opts,depth)
	depth = depth or 0
	log.info("xqueue upgrade(%s,%s)", space.name, json.encode(opts))
	if not opts.fields then error("opts.fields required",2) end
	if opts.format then
		-- todo: check if already have such format
		local format_av = box.space._space.index.name:get(space.name)[ 7 ]
		if not is_eq(format_av, opts.format) then
			space:format(opts.format)
		else
			print("formats are equal")
		end
	end

	-- This variable will be defined later
	local taken_mt

	local self = {}
	if space.xq then
		self.taken = space.xq.taken
		self._stat = space.xq._stat
		self.bysid = space.xq.bysid
		self.sid_take_fids = space.xq.sid_take_fids
		self._lock = space.xq._lock
		self.take_wait = space.xq.take_wait
		self.take_chans = space.xq.take_chans or setmetatable({}, { __mode = 'v' })
		self.put_wait = space.xq.put_wait or setmetatable({}, { __mode = 'v' })
		self._on_repl  = space.xq._on_repl
		self._on_dis = space.xq._on_dis
	else
		self.taken = {}
		self.bysid = {}
		self.sid_take_fids = {}
		-- byfid = {};
		self._lock = {}
		self.put_wait = setmetatable({}, { __mode = 'v' })
		self.take_wait = fiber.channel(0)
		self.take_chans = setmetatable({}, { __mode = 'v' })
	end
	setmetatable(self.bysid, {
		__serialize='map',
		__newindex = function(t, key, val)
			if type(val) == 'table' then
				return rawset(t, key, setmetatable(val, taken_mt))
			else
				return rawset(t, key, val)
			end
		end
	})
	self.debug = not not opts.debug
	
	if not self._default_truncate then
		self._default_truncate = space.truncate
	end
	
	local format_av = box.space._space.index.name:get(space.name)[ 7 ]
	local format = {}
	local have_format = false
	local have_runat = false
	for no,f in pairs(format_av) do
		format[ f.name ] = {
			name = f.name;
			type = f.type;
			no   = no;
		}
		format[ no ] = format[ f.name ];
		have_format = true
		self.have_format = true
	end
	for _,idx in pairs(space.index) do
		for _,part in pairs(idx.parts) do
			format[ part.fieldno ] = format[ part.fieldno ] or { no = part.fieldno }
			format[ part.fieldno ].type = part.type
		end
	end

	-- dd(format)

	-- 1. fields check
	local fields = {}
	local fieldmap = {}
	for _,f in pairs({{"status","str"},{"runat","num"},{"priority","num"},{"tube","str"},{"lock","*"}}) do
		local fname,ftype = f[1], f[2]
		local num = opts.fields[fname]
		if num then
			if type(num) == 'string' then
				if format[num] then
					fields[fname] = format[num].no;
				else
					error(string.format("unknown field %s for %s", num, fname),2 + depth)
				end
			elseif type(num) == 'number' then
				if format[num] then
					fields[fname] = num
				else
					error(string.format("unknown field %s for %s", num, fname),2 + depth)
				end
			else
				error(string.format("wrong type %s for field %s, number or string required",type(num),fname),2 + depth)
			end
			-- check type
			if format[num] and ftype ~= "*" then
				if not typeeq(format[num].type, ftype) then
					error(string.format("type mismatch for field %s, required %s, got %s",fname, ftype, format[fname].type),2+depth)
				end
			end
			fieldmap[fname] = format[num].name
		end
	end

	-- dd(fields)

	-- 2. index check

	local pk = space.index[0]
	if #pk.parts ~= 1 then
		error("Composite primary keys are not supported yet")
	end
	local pktype
	if typeeq( format[pk.parts[1].fieldno].type, 'str' ) then
		pktype = 'string'
		taken_mt = { __serialize = 'map' }
	elseif typeeq( format[pk.parts[1].fieldno].type, 'num' ) then
		pktype = 'number'
		taken_mt = {
			__serialize = 'map',
			__newindex = function(t, key, val)
				return rawset(t, tostring(ffi.cast("uint64_t", key)), val)
			end,
			__index = function(t, key)
				return rawget(t, tostring(ffi.cast("uint64_t", key)))
			end
		}
	else
		error("Unknown key type "..format[pk.parts[1].fieldno].type)
	end
	setmetatable(self.taken, taken_mt)

	local pkf = {
		no   = pk.parts[1].fieldno;
		name = format[pk.parts[1].fieldno].name;
		['type'] = pktype;
	}
	self.key = pkf
	self.fields = fields
	self.fieldmap = fieldmap
	
	if not self._stat then
		self._stat = {
			counts = {};
			transition = {};
		}
		for _, t in space:pairs(nil, { iterator = box.index.ALL }) do
			local s = t[self.fields.status]
			self._stat.counts[s] = (self._stat.counts[s] or 0LL) + 1
		end
	end
	
	function self:getkey(arg)
		local _type = type(arg)
		if _type == 'table' then
			if is_array(arg) then
				return arg[ self.key.no ]
			else
				return arg[ self.key.name ]
				-- pass
			end
		elseif _type == 'cdata' and ffi.typeof(arg) == tuple_ctype then
			return arg[ self.key.no ]
		elseif _type == self.key.type then
			return arg
		else
			error("Wrong key/task argument. Expected table or tuple or key", 3)
		end
	end

	function self:packkey(key)
		if type(key) == 'cdata' then
			return tostring(ffi.cast("uint64_t", key))
		else
			return key
		end
	end

	do
		local filter
		if fields.priority then
			filter = function(index)
				return #index.parts >= 3
					and index.parts[1].fieldno == fields.status
					and index.parts[2].fieldno == fields.priority
					and index.parts[3].fieldno == self.key.no
			end
		else
			filter = function(index)
				return #index.parts >= 2
					and index.parts[1].fieldno == fields.status
					and index.parts[2].fieldno == self.key.no
			end
		end
		for i,index in pairs(space.index) do
			if type(i) == 'number' and filter(index) then
				self.index = index
				break
			end
		end
		if not self.index then
			if fields.priority then
				error("not found index by status + priority + id",2+depth)
			else
				error("not found index by status + id",2+depth)
			end
		end

		if fields.tube then
			for n,index in pairs(space.index) do
				if type(n) == 'number' and index.parts[1].fieldno == fields.tube then
					local not_match = false
					for i = 2, #index.parts do
						if index.parts[i].fieldno ~= self.index.parts[i-1].fieldno then
							not_match = true
							break
						end
					end
					if not not_match then
						self.tube_index = index
						break
					end
				end
			end
			if not self.tube_index then
				if fields.priority then
					error("not found index by tube + status + priority + id",2+depth)
				else
					error("not found index by tube + status + id",2+depth)
				end
			end
		end

		if fields.lock then
			local lock_index
			for _,index in pairs(space.index) do
				if type(_) == 'number' and #index.parts >= 2 then
					if index.parts[1].fieldno == fields.lock and index.parts[2].fieldno == fields.status then
						-- print("found lock index", index.name)
						lock_index = index
						break
					end
				end
			end
			if not lock_index then
				error(string.format("fields.lock requires tree index with at least fields (`lock', `status', ...)"), 2+depth)
			end
			self.lock_index = lock_index
		end
	end

	local runat_index
	if fields.runat then
		for _,index in pairs(space.index) do
			if type(_) == 'number' then
				if index.parts[1].fieldno == fields.runat then
					-- print("found",index.name)
					runat_index = index
					break
				end
			end
		end
		if not runat_index then
			error(string.format("fields.runat requires tree index with this first field in it"),2+depth)
		else
			local t = runat_index:pairs({0},{iterator = box.index.GT}):nth(1)
			if t then
				if t[ self.fields.runat ] < monotonic_max_age then
					error("!!! Queue contains monotonic runat. Consider updating tasks (https://github.com/moonlibs/xqueue/issues/2)")
				end
			end
			have_runat = true
		end
	end
	self.have_runat = have_runat



	-- 3. features check

	local features = {}

	local gen_id
	opts.features = opts.features or {}
	if not opts.features.id then
		opts.features.id = 'uuid'
	end
	-- 'auto_increment' | 'time64' | 'uuid' | 'required' | function
	if opts.features.id == 'auto_increment' then
		if not (#pk.parts == 1 and typeeq( pk.parts[1].type, 'num')) then
			error("For auto_increment numeric pk is mandatory",2+depth)
		end
		local pk_fno = pk.parts[1].fieldno
		gen_id = function()
			local max = pk:max()
			if max then
				return max[pk_fno] + 1
			else
				return 1
			end
		end
	elseif opts.features.id == 'time64' then
		if not (#pk.parts == 1 and typeeq( pk.parts[1].type, 'num')) then
			error("For time64 numeric pk is mandatory",2+depth)
		end
		gen_id = function()
			local key = clock.realtime64()
			while true do
				local exists = pk:get(key)
				if not exists then
					return key
				end
				key = key + 1
			end
		end
	elseif opts.features.id == 'uuid' then
		if not (#pk.parts == 1 and typeeq( pk.parts[1].type, 'str')) then
			error("For uuid string pk is mandatory",2+depth)
		end
		local uuid = require'uuid'
		gen_id = function()
			while true do
				local key = uuid.str()
				local exists = pk:get(key)
				if not exists then
					return key
				end
			end
		end
	elseif opts.features.id == 'required' then
		-- gen_id = function()
		-- 	-- ???
		-- 	error("TODO")
		-- end
	elseif type(opts.features.id) == 'function'
		or (debug.getmetatable(opts.features.id)
		and debug.getmetatable(opts.features.id).__call)
	then
		gen_id = opts.features.id
	else
		error(string.format(
			"Wrong type for features.id %s, may be 'auto_increment' | 'time64' | 'uuid' | 'required' | function",
			opts.features.id
		), 2+depth)
	end

	if not opts.features.retval then
		opts.features.retval = have_format and 'table' or 'tuple'
	end

	if format then
		self.tuple = _table2tuple(format)
		self.table = _tuple2table(format)
	end

	if opts.features.retval == 'table' then
		self.retwrap = self.table
	else
		self.retwrap = function(t) return t end
	end

	features.buried = not not opts.features.buried
	features.keep   = not not opts.features.keep
	if opts.features.delayed then
		if not have_runat then
			error(string.format("Delayed feature requires runat field and index" ),2+depth)
		end
		features.delayed = true
	else
		features.delayed = false
	end

	if opts.features.zombie then
		if features.keep then
			error(string.format("features keep and zombie are mutually exclusive" ),2+depth)
		end
		if not have_runat then
			error(string.format("feature zombie requires runat field and index" ),2+depth)
		end

		features.zombie = true
		if type(opts.features.zombie) == 'number' then
			features.zombie_delay = opts.features.zombie
		end
	else
		features.zombie = false
	end

	if opts.features.ttl then
		if not have_runat then
			error(string.format("feature ttl requires runat field and index" ),2+depth)
		end

		features.ttl = true
		if type(opts.features.ttl) == 'number' then
			features.ttl_default = opts.features.ttl
		end
	else
		features.ttl = false
	end

	if opts.features.ttr then
		if not have_runat then
			error(string.format("feature ttr requires runat field and index" ),2+depth)
		end

		features.ttr = true
		if type(opts.features.ttr) == 'number' then
			features.ttr_default = opts.features.ttr
		end
	else
		features.ttr = false
	end

	if fields.tube then
		features.tube = true
	end

	if fields.lock then
		features.lockable = true
	end

	self.gen_id = gen_id
	self.features = features
	self.space = space.id

	function self.timeoffset(delta)
		return clock.realtime() + tonumber(delta)
	end
	function self.timeready(time)
		return time < clock.realtime()
	end
	function self.timeremaining(time)
		return time - clock.realtime()
	end
	-- self.NEVER = -1ULL
	self.NEVER = 0

	function self:keyfield(t)
		return t[pkf.no]
	end
	function self:keypack(t)
		return t[pkf.no]
	end

	function self:register_take_fid()
		local sid = box.session.id()
		local fid = fiber.id()
		if self.sid_take_fids[sid] == nil then
			self.sid_take_fids[sid] = {}
		end
		self.sid_take_fids[sid][fid] = true
	end

	function self:deregister_take_fid()
		local sid = box.session.id()
		local fid = fiber.id()
		if self.sid_take_fids[sid] == nil then
			return
		end

		self.sid_take_fids[sid][fid] = nil
	end

	function self:wakeup_locked_task(t)
		local locker_t = self.lock_index:select({ t[ self.fieldmap.lock ], 'L' }, {limit=1})[1]
		if locker_t ~= nil then
			locker_t = box.space[self.space]:update({ locker_t[ self.key.no ] }, {
				{ '=', self.fields.status, 'R' }
			})
			log.info("Unlock: L->R {%s} (by %s) from %s/sid=%s/fid=%s",
				self:getkey(locker_t), self:getkey(t), box.session.storage.peer, box.session.id(), fiber.id() )
			self:wakeup(locker_t)
		end
		return locker_t
	end

	function self:find_locker_task(lock_value)
		local locker_t
		for _, status in ipairs({'L', 'T', 'R'}) do
			locker_t = self.lock_index:select({ lock_value, status }, {limit=1})[1]
			if locker_t ~= nil then
				-- there is a task that should be processed first
				return locker_t
			end
		end
		return nil
	end

	-- Notify producer if it is still waiting us.
	-- Producer waits only for successfully processed task
	-- or for task which would never be processed.
	-- Discarding to process task depends on consumer's logic.
	-- Also task can be deleted from space by exceeding TTL.
	-- Producer should not be notified if queue is able to process task in future.
	local function notify_producer(key, task)
		local pkey = self:packkey(key)
		local wait = self.put_wait[pkey]
		if not wait then
			-- no producer
			return
		end

		if task.status == 'Z' or task.status == 'D' then
			-- task was processed
			wait.task = task
			wait.processed = true
			wait.cond:broadcast()
			return
		end
		if not space:get{key} then
			-- task is not present in space
			-- it could be TTL or :ack
			if task.status == 'T' then
				-- task was acked:
				wait.task = task
				wait.processed = true
				wait.cond:broadcast()
			elseif task.status == 'R' then
				-- task was killed by TTL
				wait.task = task
				wait.processed = false
				wait.cond:broadcast()
			end
		else
			-- task is still in space
			if task.status == 'B' then
				-- task was buried
				wait.task = task
				wait.processed = false
				wait.cond:broadcast()
			end
		end
	end

	self.ready = fiber.channel(0)

	if opts.worker then
		local workers = opts.workers or 1
		local worker = opts.worker
		for i = 1,workers do
			fiber.create(function(space,xq,i)
				local fname = space.name .. '.xq.wrk' .. tostring(i)
				if package.reload then fname = fname .. '.' .. package.reload.count end
				fiber.name(string.sub(fname,1,32))
				repeat fiber.sleep(0.001) until space.xq
				if xq.ready then xq.ready:get() end
				log.info("I am worker %s",i)
				if box.info.ro then
					log.info("Shutting down on ro instance")
					return
				end
				while box.space[space.name] and space.xq == xq do
					local task = space:take(1)
					if task then
						local key = xq:getkey(task)
						local r,e = pcall(worker,task)
						if not r then
							log.error("Worker for {%s} has error: %s", key, e)
						else
							if xq.taken[ key ] then
								space:ack(task)
							end
						end
						if xq.taken[ key ] then
							log.error("Worker for {%s} not released task", key)
							space:release(task)
						end
					end
					fiber.yield()
				end
				log.info("worker %s ended", i)
			end,space,self,i)
		end
	end

	if have_runat then
		self.runat_chan = fiber.channel(0)
		self.runat = fiber.create(function(space,xq,runat_index)
			local fname = space.name .. '.xq'
			if package.reload then fname = fname .. '.' .. package.reload.count end
			fiber.name(string.sub(fname,1,32))
			repeat fiber.sleep(0.001) until space.xq
			if xq.ready then xq.ready:get() end
			local chan = xq.runat_chan
			log.info("Runat started")
			if box.info.ro then
				log.info("Shutting down on ro instance")
				return
			end
			local maxrun = 1000
			local curwait
			local collect = {}
			while box.space[space.name] and space.xq == xq do
				local r,e = pcall(function()
					-- print("runat loop 2 ",box.time64())
					local remaining
					for _,t in runat_index:pairs({0},{iterator = box.index.GT}) do

						-- print("checking ",t)
						if xq.timeready( t[ xq.fields.runat ] ) then
							table.insert(collect,t)
						else
							remaining = xq.timeremaining(t[ xq.fields.runat ])
							break
						end
						if #collect >= maxrun then remaining = 0 break end
					end
					
					local status
					for _,t in ipairs(collect) do
						-- log.info("Runat: %s, %s", _, t)
						status = t[ xq.fields.status ]

						if status == 'W' then
							local target_status = 'R'
							if xq.features.lockable and t[ xq.fieldmap.lock ] ~= nil then
								-- check if we need to set status L or R by looking up locker task
								if xq:find_locker_task(t[ xq.fieldmap.lock ]) ~= nil then
									target_status = 'L'
								end
							end
							log.info("Runat: W->%s %s",target_status,xq:keyfield(t))
							-- TODO: default ttl?
							local u = space:update({ xq:keyfield(t) },{
								{ '=',xq.fields.status,target_status },
								{ '=',xq.fields.runat, xq.NEVER }
							})
							xq:wakeup(u)
						elseif (status == 'R' or status == 'L') and xq.features.ttl then
							local key = xq:keyfield(t)
							log.info("Runat: Kill R by ttl %s (%+0.2fs)", key, fiber.time() - t[ xq.fields.runat ])
							t = space:delete{key}
							notify_producer(key, t)
						elseif status == 'Z' and xq.features.zombie then
							log.info("Runat: Kill Zombie %s",xq:keyfield(t))
							space:delete{ xq:keyfield(t) }
						elseif status == 'T' and xq.features.ttr then
							local key = xq:keypack(t)
							local sid = xq.taken[ key ]
							local peer = peers[sid] or sid

							log.info("Runat: autorelease T->R by ttr %s taken by %s",xq:keyfield(t), peer)
							local u = space:update({ xq:keyfield(t) },{
								{ '=',xq.fields.status,'R' },
								{ '=',xq.fields.runat, xq.NEVER }
							})
							xq.taken[ key ] = nil
							if sid then
								self.bysid[ sid ][ key ] = nil
							end
							xq:wakeup(u)
						else
							log.error("Runat: unsupported status %s for %s",status, tostring(t))
							space:update({ xq:keyfield(t) },{
								{ '=',xq.fields.runat, xq.NEVER }
							})
						end
					end

					if remaining then
						if remaining >= 0 and remaining < 1 then
							return remaining
						end
					end
					return 1
				end)
				
				table_clear(collect)

				if r then
					curwait = e
				else
					curwait = 1
					log.error("Runat/ERR: %s",e)
				end
				-- log.info("Wait %0.2fs",curwait)
				if curwait == 0 then fiber.sleep(0) end
				chan:get(curwait)
			end
			log.info("Runat ended")
		end,space,self,runat_index)
	end

	local function atomic_tail(self, key, status, ...)
		self._lock[key] = nil
		if not status then
			error((...), 3)
		end
		return ...
	end
	function self:atomic(key, fun, ...)
		self._lock[key] = true
		return atomic_tail(self, key, pcall(fun, ...))
	end

	function self:check_owner(key)
		local t = space:get(key)
		if not t then
			error(string.format( "Task {%s} was not found", key ),2)
		end
		if not self.taken[key] then
			error(string.format( "Task %s not taken by any", key ),2)
		end
		if self.taken[key] ~= box.session.id() then
			error(string.format( "Task %s taken by %d. Not you (%d)", key, self.taken[key], box.session.id() ),2)
		end
		return t
	end

	function self:putback(task)
		local key = task[ self.key.no ]
		local sid = self.taken[ key ]
		if sid then
			self.taken[ key ] = nil
			if self.bysid[ sid ] then
				self.bysid[ sid ][ key ] = nil
			else
				log.error( "Task {%s} marked as taken by sid=%s but bysid is null", key, sid)
			end
		else
			log.error( "Task {%s} not marked as taken, untake by sid=%s", key, box.session.id() )
		end

		notify_producer(key, task)
	end

	function self:wakeup(t)
		if t[self.fieldmap.status] ~= 'R' then return end
		if self.fieldmap.tube then
			-- we may have consumers in the tubes:
			local tube_chan = self.take_chans[t[self.fieldmap.tube]]
			if tube_chan and tube_chan:has_readers() and tube_chan:put(true, 0) then
				-- we have successfully notified consumer
				return
			end
			-- otherwise fallback to default channel:
		end
		if self.take_wait:has_readers() then
			self.take_wait:put(true,0)
		end
	end

	function self:make_ready()
		while self.ready and self.ready:has_readers() do
			self.ready:put(true,0)
			fiber.sleep(0)
		end
		self.ready = nil
	end
	
	local meta = debug.getmetatable(space)
	for k,v in pairs(methods) do meta[k] = v end
	
	-- Triggers must set right before updating space
	-- because raising error earlier leads to trigger inconsistency
	self._on_repl = space:on_replace(function(old, new)
		local old_st, new_st
		local counts = self._stat.counts
		if old then
			old_st = old[self.fields.status]
			if counts[old_st] and counts[old_st] > 0 then
				counts[old_st] = counts[old_st] - 1
			else
				log.error(
					"Have not valid statistic by status: %s with value: %s",
					old_st, tostring(counts[old_st])
				)
			end
		else
			old_st = 'X'
		end
		
		if new then
			new_st = new[self.fields.status]
			counts[new_st] = (counts[new_st] or 0LL) + 1
			if counts[new_st] < 0 then
				log.error("Statistic overflow by task type: %s", new_st)
			end
		else
			new_st = 'X'
		end
		
		local field = old_st.."-"..new_st
		self._stat.transition[field] = (self._stat.transition[field] or 0ULL) + 1
	end, self._on_repl)
	
	self._on_dis = box.session.on_disconnect(function()
		local sid = box.session.id()
		local peer = box.session.storage.peer
		
		log.info("%s: disconnected %s, sid=%s, fid=%s", space.name, peer, sid, fiber.id() )
		box.session.storage.destroyed = true
		if self.bysid[sid] then
			local old = self.bysid[sid]
			while next(old) do
				for key,realkey in pairs(old) do
					self.taken[key] = nil
					old[key] = nil
					local t = space:get(realkey)
					if t then
						if t[ self.fields.status ] == 'T' then
							self:wakeup(space:update({ realkey }, {
								{ '=',self.fields.status,'R' },
								self.have_runat and { '=', self.fields.runat, self.NEVER } or nil
							}))
							log.info("Rst: T->R {%s}", realkey )
						else
							log.error( "Rst: %s->? {%s}: wrong status", t[self.fields.status], realkey )
						end
					else
						log.error( "Rst: {%s}: taken not found", realkey )
					end
				end
			end
			self.bysid[sid] = nil
		end

		if self.sid_take_fids[sid] then
			for fid in pairs(self.sid_take_fids[sid]) do
				log.info('Killing take fiber %d in sid %d', fid, sid)
				fiber.kill(fid)
			end

			self.sid_take_fids[sid] = nil
		end
	end, self._on_dis)
	
	rawset(space,'xq',self)

	log.info("Upgraded %s into xqueue (status=%s)", space.name, box.info.status)


	function self:starttest()
		local xq = self
		if box.info.status == 'orphan' then
			fiber.create(function()
				local x = 0
				repeat
					fiber.sleep(x/1e5)
					x = x + 1
				until box.info.status ~= 'orphan'
				self:starttest()
			end)
			return
		end
		if box.info.ro then
			log.info("Server is ro, not resetting statuses")
		else
			-- FIXME: with stable iterators add yield
			local space = box.space[self.space]
			box.begin()
			for _,t in self.index:pairs({'T'},{ iterator = box.index.EQ }) do
				local key = t[ xq.key.no ]
				if not self.taken[key] and not self._lock[key] then
					local update = {
						{ '=', self.fields.status, 'R' },
						self.have_runat and {
							'=',self.fields.runat,
							self.ttl_default and self.timeoffset( self.ttl_default ) or self.NEVER
						} or nil
					}
					space:update({key}, update)
					log.info("Start: T->R (%s)", key)
				end
			end
			box.commit()
		end
		self:make_ready()
	end
	self:starttest()
end


--[[
* `space:put`
	- `task` - table or array or tuple
		+ `table`
			* **requires** space format
			* suitable for id generation
		+ `array`
			* ignores space format
			* for id generation use `NULL` (**not** `nil`)
		+ `tuple`
			* ignores space format
			* **can't** be used with id generation
	- `attr` - table of attributes
		+ `delay` - number of seconds
			* if set, task will become `W` instead of `R` for `delay` seconds
		+ `ttl` - number of seconds
			* if set, task will be discarded after ttl seconds unless was taken
		+ `wait` - number of seconds
			* if set, callee fiber will be blocked up to `wait` seconds until task won't
			be processed or timeout reached.

```lua
box.space.myqueue:put{ name="xxx"; data="yyy"; }
box.space.myqueue:put{ "xxx","yyy" }
box.space.myqueue:put(box.tuple.new{ 1,"xxx","yyy" })

box.space.myqueue:put({ name="xxx"; data="yyy"; }, { delay = 1.5 })
box.space.myqueue:put({ name="xxx"; data="yyy"; }, { delay = 1.5; ttl = 100 })
```
]]

function methods:put(t, opts)
	local xq = self.xq
	opts = opts or {}
	if type(t) == 'table' then
		if xq.gen_id then
			if is_array(t) then
				error("Task from array creation is not implemented yet", 2)
			else
				-- pass
			end
		end
	elseif type(t) == 'cdata' and ffi.typeof(t) == tuple_ctype then
		if xq.gen_id then
			error("Can't use id generation with tuple.", 2)
		end
		error("Task from tuple creation is not implemented yet", 2)
	else
		error("Wrong argument to put. Expected table or tuple", 2)
	end
	-- here we have table with keys
	if xq.gen_id then
		t[ xq.key.name ] = xq.gen_id()
	else
		if not t[ xq.key.name ] then
			error("Primary key is mandatory",2)
		end
	end

	-- delayed or ttl or default ttl
	if opts.delay then
		if not xq.features.delayed then
			error("Feature delayed is not enabled",2)
		end
		if opts.ttl then
			error("Features ttl and delay are mutually exclusive",2)
		end
		t[ xq.fieldmap.status ] = 'W'
		t[ xq.fieldmap.runat ]  = xq.timeoffset(opts.delay)

		if opts.wait then
			error("Are you crazy? Call of :put({...}, { wait = <>, delay = <> }) looks weird", 2)
		end
	elseif opts.ttl then
		if not xq.features.ttl then
			error("Feature ttl is not enabled",2)
		end
		t[ xq.fieldmap.status ] = 'R'
		t[ xq.fieldmap.runat ]  = xq.timeoffset(opts.ttl)
	elseif xq.features.ttl_default then
		t[ xq.fieldmap.status ] = 'R'
		t[ xq.fieldmap.runat ]  = xq.timeoffset(xq.features.ttl_default)
	elseif xq.have_runat then
		t[ xq.fieldmap.status ] = 'R'
		t[ xq.fieldmap.runat ]  = xq.NEVER
	else
		t[ xq.fieldmap.status ] = 'R'
	end

	-- check lock index
	if xq.features.lockable and t[ xq.fieldmap.status ] == 'R' and t[ xq.fieldmap.lock ] ~= nil then
		-- check if we need to set status L or R by looking up tasks in L, R or T states
		if xq:find_locker_task(t[ xq.fieldmap.lock ]) ~= nil then
			t[ xq.fieldmap.status ] = 'L'
		end
	end

	local tuple = xq.tuple(t)
	local key = tuple[ xq.key.no ]

	local wait
	if opts.wait then
		wait = { cond = fiber.cond() }
		xq.put_wait[xq:packkey(key)] = wait
	end

	xq:atomic(key, function()
		t = self:insert(tuple)
	end)
	xq:wakeup(t)

	if wait then
		-- local func notify_producer will send to us some data
		local ok = wait.cond:wait(opts.wait)
		fiber.testcancel()
		if ok and wait.task then
			return xq.retwrap(wait.task), wait.processed
		end
	end
	return xq.retwrap(t)
end

--[[
* `space:wait(id, timeout)`
	- `id`:
		+ `string` | `number` - primary key
	- `timeout` - number of seconds to wait
		* callee fiber will be blocked up to `timeout` seconds until task won't
		be processed or timeout reached.
]]

local wait_for = {
	R = true,
	T = true,
	W = true,
	L = true,
}

function methods:wait(key, timeout)
	local xq = self.xq
	key = xq:getkey(key)
	local task = self:get(key)
	if not task then
		error(("Task {%s} was not found"):format(key))
	end

	local status = task[xq.fields.status]
	if not wait_for[status] then
		return xq.retwrap(task), false
	end

	local pkey = xq:packkey(key)
	local wait = xq.put_wait[pkey]
	if not wait then
		wait = { cond = fiber.cond() }
		xq.put_wait[pkey] = wait
	end

	-- local func notify_producer will send to us some data
	local ok = wait.cond:wait(timeout)
	fiber.testcancel()
	if ok and wait.task then
		return xq.retwrap(wait.task), wait.processed
	end

	return xq.retwrap(task), false
end

--[[
* `space:take(timeout)`
* `space:take(timeout, opts)`
* `space:take(opts)`
	- `timeout` - number of seconds to wait for new task
		+ choose reasonable time
		+ beware of **readahead** size (see tarantool docs)
	- `tube` - name of the tube worker wants to take task from (feature tube must be enabled)
	- returns task tuple or table (see retval) or nothing on timeout
	- *TODO*: ttr must be there
]]

function methods:take(timeout, opts)
	local xq = self.xq
	timeout = timeout or 0

	if type(timeout) == 'table' then
		opts = timeout
		timeout = opts.timeout or 0
	else
		opts = opts or {}
	end
	assert(timeout >= 0, "timeout required")

	local ttr
	if opts.ttr then
		if not xq.features.ttr then
			error("Feature ttr is not enabled",2)
		end
		ttr = opts.ttr
	elseif xq.features.ttr_default then
		ttr = xq.features.ttr_default
	end

	local index
	local start_with

	local tube_chan
	if opts.tube then
		if not xq.features.tube then
			error("Feature tube is not enabled", 2)
		end

		assert(type(opts.tube) == 'string', "opts.tube must be a string")

		index = xq.tube_index
		start_with = {opts.tube, 'R'}
		tube_chan = xq.take_chans[opts.tube] or fiber.channel()
		xq.take_chans[opts.tube] = tube_chan
	else
		index = xq.index
		start_with = {'R'}
	end

	local wait_chan = (tube_chan or xq.take_wait)
	local now = fiber.time()
	local key
	local found
	while not found do
		for _,t in index:pairs(start_with, { iterator = box.index.EQ }) do
			key = t[ xq.key.no ]
			if not xq._lock[ key ] then
				-- found key
				xq._lock[ key ] = true
				found = t
				break
			end
		end
		if not found then
			local left = (now + timeout) - fiber.time()
			if left <= 0 then goto finish end
			
			self.xq:register_take_fid()
			local ok, r = pcall(wait_chan.get, wait_chan, left)
			self.xq:deregister_take_fid()
			if not ok then 
				log.info('take finished abruptly: %s', r)
				goto finish 
			end
			if box.session.storage.destroyed then goto finish end
		end
	end
	::finish::

	-- If we were last reader from the tube
	-- we remove channel. Writers will get false
	-- on :put if they exists.
	if tube_chan and not tube_chan:has_readers() then
		tube_chan:close()
		xq.take_chans[opts.tube] = nil
	end
	if not found then return end

	local r,e = pcall(function()
		local sid = box.session.id()
		local peer = box.session.storage.peer
		
		-- print("Take ",key," for ",peer," sid=",sid, "; fid=",fiber.id() )
		if xq.debug then
			log.info("Take {%s} by %s, sid=%s, fid=%s", key, peer, sid, fiber.id())
		end

		local update = {
			{ '=', xq.fields.status, 'T' },
		}
		-- TODO: update more fields

		if ttr then
			table.insert(update,{ '=', xq.fields.runat, xq.timeoffset(ttr)})
			xq.runat_chan:put(true,0)
		end
		local t = self:update({key},update)

		if not xq.bysid[ sid ] then xq.bysid[ sid ] = {} end
		xq.taken[ key ] = sid
		xq.bysid[ sid ][ key ] = key

		return t
	end)

	xq._lock[ key ] = nil
	if not r then
		error(e)
	end
	return xq.retwrap(e)
end

--[[
* `space:release(id, [attr])`
	- `id`:
		+ `string` | `number` - primary key
		+ *TODO*: `tuple` - key will be extracted using index
		+ *TODO*: composite pk
	- `attr`
		+ `update` - table for update, like in space:update
		+ `ttl` - timeout for time to live
		+ `delay` - number of seconds
			* if set, task will become `W` instead of `R` for `delay` seconds
]]


function methods:release(key, attr)
	local xq = self.xq
	key = xq:getkey(key)

	attr = attr or {}

	local t = xq:check_owner(key)
	local old = t[ xq.fields.status ]
	local update = {}
	if attr.update then
		for _,v in pairs(attr.update) do table.insert(update,v) end
	end

	-- delayed or ttl or default ttl
	if attr.delay then
		if not xq.features.delayed then
			error("Feature delayed is not enabled",2)
		end
		if attr.ttl then
			error("Features ttl and delay are mutually exclusive",2)
		end
		table.insert(update, { '=', xq.fields.status, 'W' })
		table.insert(update, { '=', xq.fields.runat, xq.timeoffset(attr.delay) })
	elseif attr.ttl then
		if not xq.features.ttl then
			error("Feature ttl is not enabled",2)
		end
		table.insert(update, { '=', xq.fields.status, 'R' })
		table.insert(update, { '=', xq.fields.runat, xq.timeoffset(attr.ttl) })
	elseif xq.features.ttl_default then
		table.insert(update, { '=', xq.fields.status, 'R' })
		table.insert(update, { '=', xq.fields.runat, xq.timeoffset(xq.features.ttl_default) })
	elseif xq.have_runat then
		table.insert(update, { '=', xq.fields.status, 'R' })
		table.insert(update, { '=', xq.fields.runat, xq.NEVER })
	else
		table.insert(update, { '=', xq.fields.status, 'R' })
	end

	xq:atomic(key,function()
		t = self:update({key}, update)

		xq:wakeup(t)
		if xq.have_runat then
			xq.runat_chan:put(true,0)
		end

		log.info("Rel: %s->%s {%s} +%s from %s/sid=%s/fid=%s", old, t[xq.fields.status],
			key, attr.delay, box.session.storage.peer, box.session.id(), fiber.id() )
	end)
	
	xq:putback(t)
	
	return t
end

function methods:ack(key, attr)
	-- features.zombie
	-- features.keep
	local xq = self.xq
	key = xq:getkey(key)

	attr = attr or {}

	local t = xq:check_owner(key)
	local old = t[ xq.fields.status ]
	local delete = false
	local update = {}
	if attr.update then
		for _,v in pairs(attr.update) do table.insert(update,v) end
	end

	-- delayed or ttl or default ttl
	if attr.delay then
		if not xq.features.zombie then
			error("Feature zombie is not enabled",2)
		end
		table.insert(update, { '=', xq.fields.status, 'Z' })
		table.insert(update, { '=', xq.fields.runat, xq.timeoffset(attr.delay) })
	elseif xq.features.zombie then
		table.insert(update, { '=', xq.fields.status, 'Z' })
		if xq.features.zombie_delay then
			table.insert(update, { '=', xq.fields.runat, xq.timeoffset(xq.features.zombie_delay) })
		end
	elseif xq.features.keep then
		table.insert(update, { '=', xq.fields.status, 'D' })
	else
		-- do remove task
		delete = true
	end

	xq:atomic(key,function()
		if #update > 0 then
			t = self:update({key}, update)
			xq:wakeup(t)
			if xq.have_runat then
				xq.runat_chan:put(true,0)
			end
			log.info("Ack: %s->%s {%s} +%s from %s/sid=%s/fid=%s", old, t[xq.fields.status],
				key, attr.delay, box.session.storage.peer, box.session.id(), fiber.id() )
		end

		if delete then
			t = self:delete{key}
			log.info("Ack: %s->delete {%s} +%s from %s/sid=%s/fid=%s", old,
				key, attr.delay, box.session.storage.peer, box.session.id(), fiber.id() )
		end

		if xq.features.lockable and t[ xq.fieldmap.lock ] ~= nil then
			-- find any task that is locked
			xq:wakeup_locked_task(t)
		end
	end)

	xq:putback(t) -- in real drop form taken key

	return t
end

function methods:bury(key, attr)
	attr = attr or {}

	local xq = self.xq
	key = xq:getkey(key)
	local t = xq:check_owner(key)

	local update = {}
	if attr.update then
		for _,v in pairs(attr.update) do table.insert(update,v) end
	end
	table.insert(update, { '=', xq.fields.status, 'B' })

	xq:atomic(key,function()
		t = self:update({key}, update)

		xq:wakeup(t)
		if xq.have_runat then
			xq.runat_chan:put(true,0)
		end
		log.info("Bury {%s} by %s, sid=%s, fid=%s", key, box.session.storage.peer, box.session.id(), fiber.id())

		if xq.features.lockable and t[ xq.fieldmap.lock ] ~= nil then
 			xq:wakeup_locked_task(t)
		end
	end)

	xq:putback(t)
end

local function kick_task(self, key, attr)
	local xq   = self.xq
	local key  = xq:getkey(key)
	local peer = box.session.storage.peer
	local sid  = box.session.id()
	attr       = attr or {}

	if xq.debug then
		log.info("Kick {%s} by %s, sid=%s, fid=%s", key, peer, sid, fiber.id())
	end

	self:update({key}, {{ '=', xq.fields.status, 'R' }})
	xq:putback(key, attr)
end

function methods:kick(nr_tasks_or_task, attr)
	attr = attr or {}
	if type(nr_tasks_or_task) == 'number' then
		local task_n = 1
		for _, t in self.xq.index:pairs({'B'},{ iterator = box.index.EQ }) do
			if task_n > nr_tasks_or_task then break end
			kick_task(self, t, attr)
			if task_n % 500 == 0 then fiber.sleep(0) end
			task_n = task_n + 1
		end
	else
		kick_task(self, nr_tasks_or_task, attr)
	end
end

function methods:kill(key)
	local xq     = self.xq
	local key    = xq:getkey(key)
	local task   = self:get(key)
	local status = task[xq.fields.status]
	local peer   = box.session.storage.peer

	if xq.debug then
		log.info("Kill {%s} by %s, sid=%s, fid=%s", key, peer, box.session.id(), fiber.id())
	end

	self:delete(key)

	if status == 'T' then
		for sid in pairs(xq.bysid) do
			xq.bysid[sid][key] = nil
		end
		xq.taken[key] = nil
		xq._lock[key] = nil

		if xq.features.lockable and t[ xq.fieldmap.lock ] ~= nil then
			xq:wakeup_locked_task(task)
		end
	end
end

-- special remap of truncate for deliting stats and saving methods
function methods:truncate()
	local stat = self.xq._stat
	for status, _ in pairs(stat.counts) do
		stat.counts[status] = 0LL
	end
	for transition, _ in pairs(stat.transition) do
		stat.transition[transition] = nil
	end
	local ret = self.xq._default_truncate(self)
	local meta = debug.getmetatable(self)
	for k,v in pairs(methods) do meta[k] = v end
	-- Now we reset our methods after truncation because
	-- as we can see in on_replace_dd_truncate:
	-- https://github.com/tarantool/tarantool/blob/0b7cc52607b2290d2f35cc68ee1a8243988c2735/src/box/alter.cc#L2239
	-- tarantool deletes space and restores it with the same indexes
	-- but without methods
	return ret
end

local pretty_st = {
	R = "Ready",
	T = "Taken",
	W = "Waiting",
	B = "Buried",
	Z = "Zombie",
	D = "Done",
	L = "Locked",
}

function methods:stats(pretty)
	local stats = table.deepcopy(self.xq._stat)
		for s, ps in pairs(pretty_st) do
			if pretty then
				stats.counts[ps] = stats.counts[s] or 0LL
				stats.counts[s]  = nil
			else
				stats.counts[s] = stats.counts[s] or 0LL
			end
		end
	return stats
end

setmetatable(M,{
	__call = function(M, space, opts)
		M.upgrade(space,opts,1)
	end
})

return M
