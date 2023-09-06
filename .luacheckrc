std = "luajit"
codes = true
globals = {
	-- Tarantool variable:
	"box",
	"table.deepcopy",
	"dostring",

	-- package reload:
	"package.reload",
}

include_files = {
	"xqueue.lua"
}

max_line_length = 140

ignore = {
	"431", -- shadowing upvalue self
	"432", -- shadowing upvalue argument
	"542", -- empty if branch
}
