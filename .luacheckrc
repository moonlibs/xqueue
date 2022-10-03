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

ignore = {
	"211",
	"212",
	"431",
	"432",
	"542",
	"611",
-- 	"631",
}
