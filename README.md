# lucdb - Lua binding to [howerj's cdb](https://github.com/howerj/cdb)

Allows one to read and create constant databases from Lua. Free of external dependencies.
Heavily inspired by [lua-tinycdb](https://github.com/asb/lua-tinycdb/).

This is not only my very first proper Lua binding, but also my very first C project --- it is as much a learning exercise as a project I'd like to see through.

Probably not a good idea to use this as it is right now; it's still largely untested, and as of this writing, the upstream library appears to be seeing some work on a few important features.

At the moment, this binding only works with Lua 5.1; however, the goal is to support all the newer releases plus LuaJIT.


## Documentation

TBA. Note that the interface is still very unstable and bound to change.


## Installation

To build and install locally: `luarocks make`

To build, install locally and test: `luarocks make && luarocks test`

Will be published to Luarocks when it is ready.


## License

Will upload a license file later, for now assume MIT.
