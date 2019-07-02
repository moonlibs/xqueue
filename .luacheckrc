std = "luajit"
globals = {
	-- Tarantool variable:
	"box",
	"table.deepcopy",
	"dostring",

	-- package reload:
	"package.reload",
}

ignore = {
-- 	"211",
	"611",
-- 	"631",
-- 	"542"
}
