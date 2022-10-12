---@class Task
---@field _queue Queue
local M = require 'obj'.class({}, 'task')

function M:_init(opts)
	for k,v in pairs(opts) do self[k] = v end
	assert(self._queue, "Queue must be passed")
end

function M:__tostring()
	return ("Task <%s:%s:%s:%d>"):format(self.id, self.kind, self.dedup, self.attempt)
end

function M:fetch()
	return self._queue:get({ self.id })
end

---backoffs task
---@param reason string
---@return Task
function M:backoff(reason)
	return self._queue:backoff(self, reason)
end

---Putbacks task
---@param reason string reason of release
---@param opts { delay: number?, reason: string? } options of release
---@return Task
function M:release(reason, opts)
	opts = opts or {}
	opts.reason = reason
	return self._queue:release(self, opts)
end

---Acknowledges task processing
---@param result map
---@return Task
function M:ack(result, opts)
	return self._queue:ack(self, result, opts)
end

---Final fail for task
---@param reason string Final Bury reason must be given
---@return Task
function M:bury(reason)
	return self._queue:bury(self, reason)
end

return M
