package = "xqueue"
version = "scm-6"
source = {
   url = "git://github.com/ktsstudio/xqueue.git",
   branch = "locks",
}
description = {
   summary = "Package for loading external lua config",
   homepage = "https://github.com/moonlibs/xqueue.git",
   license = "BSD"
}
dependencies = {
   "lua ~> 5.1"
}
build = {
   type = "builtin",
   modules = {
      xqueue = "xqueue.lua"
   }
}
