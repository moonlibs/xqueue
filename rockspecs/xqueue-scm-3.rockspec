package = 'xqueue'
version = 'scm-3'
source  = {
    url    = 'git+https://github.com/moonlibs/xqueue.git',
    branch = 'v3',
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
