package="lucdb"
version="dev-1"
source = {
   url = "git+http://github.com/RiskoZoSlovenska/lucdb",
}
description = {
   summary = "Self-contained 5.1/5.2+ binding to howerj's cdb, with 64-bit support.",
   homepage = "http://github.com/RiskoZoSlovenska/lucdb",
   license = "MIT",
}
dependencies = {
   "lua = 5.1",
}
build = {
   type = "builtin",
   modules = {
      lucdb = {
         "cdb/cdb.c",
         "cdb/host.c",
         "lucdb.c",
      }
   }
}
