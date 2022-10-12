local spacer = require 'spacer'

---@class Task
---@field id uint64_t ID of the Task
---@field nice number Nice of the task (the lower, the more priority it has for consumer)
---@field status "R"|"W"|"T"|"D"|"Z"|"B" status of the task
---@field tube string name of the tube of the task (name of the consumer)
---@field kind string type of the task
---@field dedup string Deduplication ID, can be used with {dedup, kind}
---@field attempt number amount of take attempts for the task
---@field runat number timestamp to trigger runat fiber (xqueue specific)
---@field mtime number timestamp of last processing
---@field payload map request of the task
---@field result map result of the task
---@field error map? error (temporary or final) of single task processing.

spacer.create_space('queue', {
    { name = 'id',      type = 'unsigned' },
    { name = 'nice',    type = 'number'   },
    { name = 'status',  type = 'string'   },
    { name = 'tube',    type = 'string', is_nullable = true },
    { name = 'kind',    type = 'string'   },
	{ name = 'dedup',   type = 'string'   },
    { name = 'attempt', type = 'number'   },
    { name = 'runat',   type = 'number'   },
	{ name = 'mtime',   type = 'number', is_nullable = true },
    { name = 'payload', type = 'map'      },
    { name = 'result',  type = 'map'      },
	{ name = 'error',   type = 'map', is_nullable = true },
}, {
    { name = 'id',         type = 'TREE', parts = { 'id' } },
	{ name = 'xq',         type = 'TREE', parts = { 'status', 'nice', 'id' } },
    { name = 'tube',       type = 'TREE', parts = { 'tube', 'status', 'nice', 'id' } },
    { name = 'runat',      type = 'TREE', parts = { 'runat', 'id' } },
	{ name = 'dedup_kind', type = 'TREE', parts = { 'dedup', 'kind' } },
})

