### TODO

* reload/upgrade and feature switch
* composite primary keys
* switch - turn non-taken task into any state
* snatch - take away Taken task from its owner and turn it into R/W
* restart (renew, requeue?) - take task from W/D/Z states and put it into R/W
* it seems, that ttl/ttr/bury requires attrs field


# Status

* **R** - ready - task is ready to be taken
	- created by put without delay
	- turned on by release/kick without delay
	- turned on from W when delay passed
	- deleted after ttl if ttl is enabled

* **T** - taken - task is taken by consumer. may not be taken by other.
	- turned into R after ttr if ttr is enabled

* **W** - waiting - task is not ready to be taken. waiting for its `delay`
	- requires `runat`
	- `delay` may be set during put, release, kick
	- turned into R after delay

* **B** - buried - task was temporary discarded from queue by consumer
	- may be revived using kick by administrator
	- use it in unpredicted conditions, when man intervention is required
	- Without mandatory monitoring (stats.buried) usage of buried is useless and awry

* **Z** - zombie - task was processed and ack'ed and **temporary** kept for delay

* **D** - done - task was processed and ack'ed and **permanently** left in database
	- enabled when keep feature is set

# Interface

## Creator methods

* `.upgrade(space, options)`

Imbue space with power of queue

```lua
M.upgrade(space, {
	format = {
		-- space format. applied to space.format() if passed
	},
	fields = {
		-- id is always taken from pk
		status   = 'status_field_name'    | status_field_no,
		runat    = 'runat_field_name'     | runat_field_no,
		priority = 'priority_field_name'  | priority_field_no,
	},
	features = {
		id = 'auto_increment' | 'time64' | 'uuid' | 'required' | function
			-- auto_increment - if pk is number, then use it for auto_increment
			-- uuid - if pk is string, then use uuid for id
			-- required - primary key MUST be present in tuple during put
			-- function - funciton will be called to aquire id for task
		retval = 'table' | 'tuple'
			-- table requires space format. default if applicable. a bit slower

		buried = true,           -- if true, support bury/kick
		delayed = true,          -- if true, support delayed tasks, requires `runat`

		keep = true,             -- if true, keep ack'ed tasks in [D]one state, instead of deleting
			-- mutually exclusive with zombie

		zombie = true|number,    -- requires `runat` field
			-- if number, then with default zombie delay, otherwise only if set delay during ack
			-- mutually exclusive with keep

		ttl    = true|number,    -- requires `runat` field
			-- Time To Live. Task is expired unless taken within time
			-- if number, then with default ttl, otherwise only if set during put/release
		ttr    = true|number,    -- requires `runat` field
			-- Time To Release. Task is returned into [R]eady unless processed (turned to ack|release from taken) within time
			-- if number, then with default ttl, otherwise only if set during take
	},
	-- Set tubes for which statistics collector will be enabled
	tube_stats = { 'tube-1', 'tube-2' },
})
```

## Producer methods

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

```lua
box.space.myqueue:put{ name="xxx"; data="yyy"; }
box.space.myqueue:put{ "xxx","yyy" }
box.space.myqueue:put(box.tuple.new{ 1,"xxx","yyy" })

box.space.myqueue:put({ name="xxx"; data="yyy"; }, { delay = 1.5 })
box.space.myqueue:put({ name="xxx"; data="yyy"; }, { delay = 1.5; ttl = 100 })
```

## Consumer methods

* `space:take(timeout)`
	- `timeout` - number of seconds to wait for new task
		+ choose reasonable time
		+ beware of **readahead** size (see tarantool docs)
	- returns task tuple or table (see retval) or nothing on timeout
	- *TODO*: ttr must be there

* `space:ack(id, [attr])`
	- `id`:
		+ `string` | `number` - primary key
		+ `tuple` - key will be extracted using index
		+ *TODO*: composite pk
	- `attr`:
		+ `update` - table for update, like in space:update
		+ `delay` - number of seconds
			* applicable only when `zombie` enabled
			* if `zombie` is enabled, delay deletion for delay time
	- returns task tuple or table (see retval)

* `space:release(id, [attr])`
	- `id` - as in `:ack`
	- `attr`
		+ `update` - table for update, like in space:update
		+ `ttl` - timeout for time to live
		+ `delay` - number of seconds
			* if set, task will become `W` instead of `R` for `delay` seconds

* `space:bury(id, [attr])`
	- `id` - as in `:ack`
	- `attr`
		+ `update` - table for update, like in space:update
		+ `ttl` - timeout for time to live (?)

## Admin methods

* `space:dig(id, [attr])` - dig out task from buried state
	- `id` - as in `:ack`
	- `attr`
		+ `update` - table for update, like in space:update
		+ `ttl` - timeout for time to live
		+ `delay` - number of seconds
			* if set, task will become `W` instead of `R` for `delay` seconds
	- returns task tuple or table (see retval)

* `space:plow(N, [attr])` - dig out N oldest tasks from buried state
	- `N` - number. To dig out all buried tasks set infinity or nil
	- `attr` - attrs for every task, like in dig

* `space:queue_stats()`
	- returns table
		+ *TODO*

