#!/usr/bin/env tarantool

box.cfg{ listen = '127.0.0.1:3301', wal_mode = 'none' }
box.schema.user.grant('guest', 'super', nil, nil, { if_not_exists = true })
box.schema.space.create('queue', { if_not_exists = true }):create_index('pri', { if_not_exists = true, parts = {1, 'string'} })
box.space.queue:create_index('xq', { parts = { { 2, 'string' }, { 1, 'string' } }, if_not_exists = true })
box.space.queue:create_index('runat', { parts = { { 3, 'number' }, { 1, 'string' } }, if_not_exists = true })

require 'xqueue'.upgrade(box.space.queue, {
	format = {
		{ name = 'id',      type = 'string' },
		{ name = 'status',  type = 'string' },
		{ name = 'runat',   type = 'number' },
		{ name = 'payload', type = '*'      },
	},
	debug = true,
	fields = {
		status = 'status',
		runat  = 'runat',
	},
	features = {
		id = 'uuid',
		delayed = true,
	},
})

require 'console'.start() os.exit(0)
