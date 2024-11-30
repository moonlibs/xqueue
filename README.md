# xQueue

[![Coverage Status](https://coveralls.io/repos/github/moonlibs/xqueue/badge.svg?branch=master)](https://coveralls.io/github/moonlibs/xqueue?branch=master)

Add power of the Queue into Tarantool Space

Latest Release: 5.0.0

Backward compatibility rockspec: rockspecs/xqueue-scm-5.rockspec

Always latest rockspec: rockspecs/xqueue-dev-1.rockspec

## Status

* **R** - ready - task is ready to be taken
  * created by put without delay
  * turned on by release/kick without delay
  * turned on from W when delay passed
  * deleted after ttl if ttl is enabled

* **T** - taken - task is taken by consumer. may not be taken by other.
  * turned into R after ttr if ttr is enabled

* **W** - waiting - task is not ready to be taken. waiting for its `delay`
  * requires `runat`
  * `delay` may be set during put, release, kick
  * turned into R after delay

* **B** - buried - task was temporary discarded from queue by consumer
  * may be revived using kick by administrator
  * use it in unpredicted conditions, when man intervention is required
  * Without mandatory monitoring (stats.buried) usage of buried is useless and awry

* **Z** - zombie - task was processed and ack'ed and **temporary** kept for delay

* **D** - done - task was processed and ack'ed and **permanently** left in database
  * enabled when keep feature is set

## Interface

### Creator methods

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
        id = 'auto_increment' | 'time64' | 'uuid' | 'required' | function() ... return task_id end
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

        not_check_session = true, -- requires 'ttr'
        -- if true, not check session on ack/bury/release and not release task on disconnect
    },
    -- Set tubes for which statistics collector will be enabled
    tube_stats = { 'tube-1', 'tube-2' },
})
```

### Producer methods

* `space:put`
  * `task` - table or array or tuple
    * `table`
      * **requires** space format
      * suitable for id generation
    * `array`
      * ignores space format
      * for id generation use `NULL` (**not** `nil`)
    * `tuple`
      * ignores space format
      * **can't** be used with id generation
  * `attr` - table of attributes
    * `delay` - number of seconds
      * if set, task will become `W` instead of `R` for `delay` seconds
    * `ttl` - number of seconds
      * if set, task will be discarded after ttl seconds unless was taken

```lua
box.space.myqueue:put{ name="xxx"; data="yyy"; }
box.space.myqueue:put{ "xxx","yyy" }
box.space.myqueue:put(box.tuple.new{ 1,"xxx","yyy" })

box.space.myqueue:put({ name="xxx"; data="yyy"; }, { delay = 1.5 })
box.space.myqueue:put({ name="xxx"; data="yyy"; }, { delay = 1.5; ttl = 100 })
```

### Consumer methods

* `space:take(timeout)`
  * `timeout` - number of seconds to wait for new task
    * choose reasonable time
    * beware of **readahead** size (see tarantool docs)
  * returns task tuple or table (see retval) or nothing on timeout
  * *TODO*: ttr must be there

* `space:ack(id, [attr])`
  * `id`:
    * `string` | `number` - primary key
    * `tuple` - key will be extracted using index
    * *TODO*: composite pk
  * `attr`:
    * `update` - table for update, like in space:update
    * `delay` - number of seconds
      * applicable only when `zombie` enabled
      * if `zombie` is enabled, delay deletion for delay time
  * returns task tuple or table (see retval)

* `space:release(id, [attr])`
  * `id` - as in `:ack`
  * `attr`
    * `update` - table for update, like in space:update
    * `ttl` - timeout for time to live
    * `delay` - number of seconds
      * if set, task will become `W` instead of `R` for `delay` seconds

* `space:bury(id, [attr])`
  * `id` - as in `:ack`
  * `attr`
    * `update` - table for update, like in space:update
    * `ttl` - timeout for time to live (?)

* `space:wait(id, [timeout])`
  * `id` - as in `:ack`
  * `timeout` - number of seconds to wait for the task processing
  * returns task tuple or table (see retval) and boolean `was_processed` flag

### Admin methods

* `space:dig(id, [attr])` - dig out task from buried state
  * `id` - as in `:ack`
  * `attr`
    * `update` - table for update, like in space:update
    * `ttl` - timeout for time to live
    * `delay` - number of seconds
      * if set, task will become `W` instead of `R` for `delay` seconds
  * returns task tuple or table (see retval)

* `space:kill(id)` - kill the task when it was taken
  * `id` - as in `:ack`
  * returns task tuple or table (see retval)

* `space:dig(N, [attr])` - dig out N oldest tasks from buried state
  * `N` - number. To dig out all buried tasks set infinity or nil
  * `attr` - attrs for every task, like in dig

* `space:stats()`
  * returns table

  ```yaml
  tarantool> box.space.queue:stats()
  ---
  - tube:
      B:
        counts: {'T': 0, 'W': 0, 'R': 0, 'D': 0, 'Z': 0, 'B': 0}
        transition:
          X-R: 6
          R-T: 7
          T-S: 3
          T-X: 4
      A:
        counts: {'B': 0, 'W': 0, 'D': 0, 'T': 0, 'Z': 0, 'R': 0}
        transition: []
      C:
        counts: {'T': 0, 'W': 0, 'R': 0, 'D': 0, 'Z': 0, 'B': 0}
        transition:
          T-R: 16
          R-T: 19
          S-R: 3
          T-X: 3
    transition:
      X-R: 6
      R-T: 26
      T-R: 19
      T-X: 7
    counts: {'T': 0, 'W': 0, 'R': 0, 'D': 0, 'Z': 0, 'B': 0}
  ...
  ```

## Explanation of Statuses in :stats()

| Status | Description                                                                 |
|--------|-----------------------------------------------------------------------------|
| X      | Special status, means task went to void, or appeared from void (makes sense in transitions) |
| R      | (Ready) Task is Ready to be Taken                                           |
| W      | (Waiting) Task processing was delayed by producer or consumer               |
| T      | (Taken) Task has been taken by consumer                                     |
| B      | (Buried) Fatal error happened during task processing (and consumer buried it) |
| Z      | (Zombie) Task was processed successfully and left in the Queue for zombie_delay |
| D      | (Done) Task was processed successfully and left in the Queue                |
| S      | Extra status for tube transition                                            |

## Status Transitions

| Transition | Description                                                                                 |
|------------|---------------------------------------------------------------------------------------------|
| X -> R     | Task was put into the Queue into Ready state (method :put)                                  |
| X -> W     | Task was put into the Queue into Waiting state (method :put with specified delay)           |
| W -> R     | Task was scheduled by runat mechanism of the Queue and became ready to be Taken by consumer |
| R -> T     | Task was taken by consumer                                                                  |
| T -> W     | Consumer returned task into the Queue and delayed its execution                             |
| T -> R     | Consumer returned task into the Queue for immediate execution                               |
| T -> X     | Consumer acknowledged successful execution of the Task, and task was removed from the Queue |
| T -> Z     | Consumer acknowledged successful execution of the Task, and task was moved to Zombie state for zombie_delay timeout |
| T -> D     | Consumer acknowledged successful execution of the Task, and task was moved to Done status and will be left in the Queue forever |
| T -> S     | Task was moved into another tube                                                            |
| S -> R     | Task was moved from another tube to be Ready                                                |
| S -> W     | Task was moved from another tube to be Waiting                                              |
| S -> D     | Task was moved from another tube to be Done                                                 |
| S -> B     | Task was moved from another tube to be Buried                                               |

## Benchmarks

```bash
❯ .rocks/bin/luabench -d 1000000x
Tarantool version: Tarantool 2.10.7-0-g60f7e18
Tarantool build: Darwin-arm64-RelWithDebInfo (static)
Tarantool build flags:  -Wno-unknown-pragmas -fexceptions -funwind-tables -fno-common -Wformat -Wformat-security -Werror=format-security -fstack-protector-strong -fPIC -fmacro-prefix-map=/var/folders/8x/1m5v3n6d4mn62g9w_65vvt_r0000gn/T/tarantool_install1565297302=. -std=c11 -Wall -Wextra -Wno-strict-aliasing -Wno-char-subscripts -Wno-gnu-alignof-expression -Wno-cast-function-type
CPU: Apple M1 @ 8
JIT: Enabled
JIT: fold cse dce fwd dse narrow loop abc sink fuse
Duration: 1000000 iters

--- BENCH: 001_put_take_bench::bench_producer:producer-1
 1000000             44046 ns/op             22703 op/s     4137 B/op   +3946.04MB

--- BENCH: 001_put_take_bench::bench_producer:producer-2
 1000000             27519 ns/op             36339 op/s     4282 B/op   +4084.35MB

--- BENCH: 001_put_take_bench::bench_producer:producer-4
 1000000             21510 ns/op             46491 op/s     4500 B/op   +4291.69MB

--- BENCH: 001_put_take_bench::bench_producer:producer-8
 1000000             20804 ns/op             48068 op/s     4379 B/op   +4176.26MB

--- BENCH: 001_put_take_bench::bench_producer:producer-12
 1000000             20383 ns/op             49062 op/s     4372 B/op   +4169.59MB

--- BENCH: 001_put_take_bench::bench_producer:producer-16
 1000000             21495 ns/op             46523 op/s     4215 B/op   +4019.98MB

--- BENCH: 001_put_take_bench::bench_producer:producer-20
 1000000             23676 ns/op             42238 op/s     4258 B/op   +4061.49MB

--- BENCH: 001_put_take_bench::bench_producer:producer-24
 1000000             24456 ns/op             40891 op/s     4274 B/op   +4076.65MB
```

## TODO

* reload/upgrade and feature switch
* composite primary keys
* switch - turn non-taken task into any state
* snatch - take away Taken task from its owner and turn it into R/W
* restart (renew, requeue?) - take task from W/D/Z states and put it into R/W
* it seems, that ttl/ttr/bury requires attrs field
