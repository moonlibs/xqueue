---@diagnostic disable: inject-field
local t = require 'luatest' --[[@as luatest]]
local replicaset = require 'luatest.replica_set'

local group_config = {
	raft = {
		box_cfg = {
			log_level = 5,
			replication_timeout = 0.1,
			replication_connect_timeout = 10,
			replication_sync_lag = 0.01,
			election_mode = 'candidate',
			replication_synchro_quorum = 3,
		},
	},
	repl = {
		box_cfg = {
			log_level = 5,
			replication_timeout = 1.1,
			replication_connect_timeout = 10,
			replication_sync_lag = 0.01,
			replication_connect_quorum = 3,
		},
	},
}

local Server = t.Server
local g = t.group('replicaset', {
	{ name = 'raft' },
	{ name = 'repl' },
})

---@return boolean
local function setup_queue(opts)
	local log = require 'log'
	local fiber = require 'fiber'

	local function await_on_ro(func)
		while box.info.ro do
			if pcall(func) then break end
			fiber.sleep(0.01)
		end
		return box.info.ro
	end
	while box.info.ro and not box.space[opts.name] do
		-- we're ro and no space found
		log.info("i'm ro and no space")
		if box.ctl.wait_rw then
			pcall(box.ctl.wait_rw, 0.01)
		else
			fiber.sleep(0.01)
		end
		-- we're rw, but don't know whether space exists
		if not box.info.ro then
			log.info("i'm rw will create spaces")
			break
		end
		-- we're ro so we have to await box.space[opts.name]
	end
	log.info("creating space: i'm %s", box.info.ro and 'ro' or 'rw')

	local space = box.schema.space.create(opts.name, {
		if_not_exists = true,
		format = opts.format,
	})

	await_on_ro(function() space.index.primary:count() end)
	space:create_index('primary', { parts = {'id'}, if_not_exists = true })

	if opts.fields.runat then
		await_on_ro(function() space.index.runat:count() end)
		space:create_index('runat', { parts = {'runat', 'id'}, if_not_exists = true })
	end

	await_on_ro(function() space.index.xq:count() end)
	if opts.fields.priority then
		space:create_index('xq', {
			if_not_exists = true,
			parts = {'status', tostring(opts.fields.priority), 'id'}
		})
	else
		space:create_index('xq', {
			if_not_exists = true,
			parts = {'status', 'id'}
		})
	end

	if opts.fields.tube then
		await_on_ro(function() space.index.tube:count() end)
		if opts.fields.priority then
			space:create_index('tube', {
				if_not_exists = true,
				parts = { 'tube', 'status', tostring(opts.fields.priority), 'id' }
			})
		else
			space:create_index('tube', {
				if_not_exists = true,
				parts = { 'tube', 'status', 'id' }
			})
		end
	end

	require'xqueue'.upgrade(space, opts)
	return true
end

local fio = require 'fio'
local fiber = require 'fiber'
local log = require 'log'

g.after_each(function(test)
	fiber.sleep(1.5) -- give some time to collect coverage
	test.ctx.replica_set:stop()
end)

local function v2num(str)
	str = str or rawget(_G, '_TARANTOOL')
	local maj, min = unpack(str:split('.'))
	return tonumber(maj)*1000 + tonumber(min)
end

function g.test_start(test)
	test.ctx = {}
	local params = test.params

	if v2num() < v2num('2.8') and params.name == 'raft' then
		t.skip('Raft is not supported by this Tarantool version')
		return
	end

	local extend = function(result, template)
		for k, v in pairs(template) do if result[k] == nil then result[k] = v end end
		return result
	end

	local data_dir = os.getenv('LUATEST_LUACOV_ROOT')
	local replica_set = replicaset:new({})

	---@type BoxCfg
	local box_cfg = extend({
		replication = {
			Server.build_listen_uri('replica1', replica_set.id),
			Server.build_listen_uri('replica2', replica_set.id),
			Server.build_listen_uri('replica3', replica_set.id),
		},
	}, group_config[params.name].box_cfg)

	local server_template = {
		datadir = data_dir,
		coverage_report = true,
	}

	replica_set:build_and_add_server(extend({ alias = 'replica1', box_cfg = box_cfg }, server_template))
	replica_set:build_and_add_server(extend({
		alias = 'replica2',
		box_cfg = extend({
			read_only = params.name == 'repl'
		}, box_cfg),
	}, server_template))
	replica_set:build_and_add_server(extend({
		alias = 'replica3',
		box_cfg = extend({
			read_only = params.name == 'repl'
		}, box_cfg),
	}, server_template))
	replica_set:start()
	replica_set:wait_for_fullmesh()

	test.ctx.replica_set = replica_set

	local simpleq = {
		name = 'simpleq',
		format = {
			{ name = 'id',      type = 'string' },
			{ name = 'status',  type = 'string' },
			{ name = 'runat',   type = 'number' },
			{ name = 'payload', type = 'any'    },
		},
		features = {
			id = 'uuid',
			keep = false,
			delayed = true,
		},
		fields = {
			id = 'id',
			status = 'status',
			runat = 'runat',
		},
		retval = 'table',
	} --[[@as xqueue.upgrade.options]]

	local had_error
	local await = {}
	for _, srv in pairs(replica_set.servers) do
		---@cast srv luatest.server
		local fib = fiber.create(function()
			fiber.self():set_joinable(true)
			local ok, err = pcall(srv.exec, srv, setup_queue, {simpleq}, { timeout = 20 })
			if not ok then
				had_error = err
				log.error("%s: %s", srv.alias, err)
			end
		end)
		table.insert(await, fib)
	end
	for _, f in ipairs(await) do
		fiber.join(f)
	end

	t.assert_not(had_error, "setup of queue without errors")

	local rw = replica_set:get_leader() --[[@as luatest.server]]
	local cookie = require'uuid'.str()
	local task = rw:call('box.space.simpleq:put', {{payload = {cookie = cookie}}, { delay = 5 } })
	t.assert(task, "task was returned")

	local SWITCH_TIMEOUT = 10

	if params.name == 'raft' then
		local deadline = fiber.time() + SWITCH_TIMEOUT
		local new_rw
		repeat
			rw:exec(function() box.ctl.demote() end)
			fiber.sleep(0.5)
			new_rw = replica_set:get_leader()
			if fiber.time() > deadline then break end
		until new_rw and rw.alias ~= new_rw.alias

		if not new_rw or new_rw.alias == rw.alias then
			t.fail("box.ctl.demote() failed to elect different leader")
		end
		rw = new_rw
	elseif params.name == 'repl' then
		local leader_vclock = rw:exec(function()
			box.cfg{read_only = true}
			require 'fiber'.sleep(0)
			return box.info.vclock
		end)

		local new_rw
		for _, server in ipairs(replica_set.servers) do
			if server ~= rw then
				new_rw = server
			end
		end

		t.assert_not_equals(new_rw.alias, rw.alias, "candidate to switch must be choosen")

		new_rw:exec(function(vclock, timeout)
			local fib = require 'fiber'

			local eq_vclock = function()
				for n, lsn in pairs(vclock) do
					if (box.info.vclock[n] or 0) ~= lsn then return false end
				end
				return true
			end

			local deadline = fib.time() + timeout
			while not eq_vclock() and deadline > fib.time() do fib.sleep(0.001) end
			assert(eq_vclock, "failed to await vclock")

			box.cfg{read_only = false}
			if box.ctl.promote then
				-- take control over synchro queue
				box.ctl.promote()
			end
		end, {leader_vclock, SWITCH_TIMEOUT - 0.1}, { timeout = SWITCH_TIMEOUT })
		rw = new_rw
	end

	do
		local trw = replica_set:get_leader()
		t.assert_equals(trw.alias, rw.alias, "after rw-switch luatest succesfully derived new leader")
	end

	local task = rw:call('box.space.simpleq:take', { 5 }, { timeout = 6 })
	t.assert(task, "delayed task has been succesfully taken from new leader")
	t.assert_equals(task.payload.cookie, cookie, "task.cookie is valid")

end
