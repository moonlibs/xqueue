#!/usr/bin/env tarantool

local netbox = require 'net.box'
local fiber = require 'fiber'
local log = require 'log'

local clis = {}
for w = 1, 2 do
	clis[w] = netbox.connect('127.0.0.1:3301')
	fiber.create(function()
		log.info("Received: %s %s", w, clis[w]:eval('return box.space.queue:take(60)', {}, { timeout = 65 }).payload)
	end)
end

clis[1]:close()
