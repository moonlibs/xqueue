package = 'xqueue'
version = 'scm-1'
source  = {
    url    = 'git://github.com/moonlibs/xqueue.git',
    branch = 'broken',
}
description = {
    summary  = "Package for loading external lua config",
    homepage = 'https://github.com/moonlibs/xqueue.git',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',
    modules = {
        ['xqueue'] = 'xqueue.lua'
    }
}

-- vim: syntax=lua
