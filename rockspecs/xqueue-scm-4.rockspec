package = 'xqueue'
version = 'scm-4'
source  = {
    url    = 'git://github.com/moonlibs/xqueue.git',
    branch = 'master',
}
description = {
    summary  = "Package for loading external lua config",
    homepage = 'https://github.com/moonlibs/xqueue.git',
    license  = 'BSD',
}
-- external_dependencies = {
--     TARANTOOL = {
--         header = 'tarantool/module.h';
--     }
-- }
dependencies = {
    'lua ~> 5.1';
}
build = {
    type = 'builtin',
    modules = {
        ['xqueue'] = 'xqueue.lua'
    }
}

-- vim: syntax=lua
