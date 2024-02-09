#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "cdb/cdb.h"
#include "cdb/host.h"

typedef struct lucdb_wrap {
	cdb_t *handle;
	int mode;
	bool open;
} lucdb_wrap;

#define WRAP_NAME "lucdb.handle"
#define MAX_WORD_SIZE (sizeof(cdb_word_t) * 8)


static inline const char* mode_name(int mode) {
	return mode == CDB_RO_MODE ? "read-only" : "read-write";
}

static inline lucdb_wrap* get_wrap(lua_State *L, int index) {
	return (lucdb_wrap *)luaL_checkudata(L, index, WRAP_NAME);
}

static inline lucdb_wrap* check_mode(lua_State *L, int mode, lucdb_wrap *obj) {	
	if (obj->mode != mode) {
		luaL_error(L, "operation not supported by handle opened in %s mode", mode_name(obj->mode));
	}
	return obj;
}

static inline lucdb_wrap* check_not_closed(lua_State *L, lucdb_wrap *obj) {
	if (!obj->open) {
		luaL_error(L, "handle has been closed");
	}
	return obj;
}

static inline lucdb_wrap* get_open_reader(lua_State *L, int index) {
	return check_not_closed(L, check_mode(L, CDB_RO_MODE, get_wrap(L, index)));
}

static inline lucdb_wrap* get_open_writer(lua_State *L, int index) {
	return check_not_closed(L, check_mode(L, CDB_RW_MODE, get_wrap(L, index)));
}


static inline void safe_close_wrap(lucdb_wrap *wrap) {
	if (wrap->open) {
		cdb_close(wrap->handle);
		wrap->open = false;
	}
}

// Push and return a `nil` followed by an error message (taken from the current
// status of the handle). This also CLOSES THE HANDLE!
static inline int err_ret(lua_State *L, lucdb_wrap *wrap) {
	lua_pushnil(L);
	lua_pushfstring(L, "unknown database error (%d)", cdb_status(wrap->handle));
	safe_close_wrap(wrap);
	return 2;
}

// Push and return a `true` followed by a `nil`.
static inline int ok_ret(lua_State *L) {
	lua_pushboolean(L, true);
	lua_pushnil(L);
	return 2;
}

// Reads the string at index `index` and returns a `cdb_buffer_t` holding it,
// throwing an error if there is no string.
static cdb_buffer_t buffer_from_lua_string(lua_State *L, int index) {
	size_t len;
	const char* str = luaL_checklstring(L, index, &len);
	cdb_buffer_t value = { .buffer = str, .length = len };
	return value;
}

// Seeks, reads and pushes the value at `cdb_file_pos_t`
static bool read_and_push(lua_State *L, cdb_t *handle, const cdb_file_pos_t *location) {
	void *data = malloc(location->length);

	if (cdb_seek(handle, location->position) < 0 || cdb_read(handle, data, location->length) < 0) {
		return false;
	}

	lua_pushlstring(L, data, location->length);
	free(data);
	return true;
}


static int create_wrap(lua_State *L, int mode) {
	size_t name_len;
	const char *name = luaL_checklstring(L, 1, &name_len);
	if (strlen(name) < name_len) {
		return luaL_error(L, "filename must not contain zero characters");
	}

	// Parse options
	cdb_options_t options = cdb_host_options;
	if (!lua_isnoneornil(L, 2)) {
		luaL_checktype(L, 2, LUA_TTABLE);
		lua_getfield(L, 2, "word_size");

		lua_Integer size = luaL_optinteger(L, -1, 0);
		if (!(size == 0 || size == 16 || size == 32 || size == 64)) {
			return luaL_error(L, "word_size must be one of 0, 16, 32 or 64 (got %d)", size);
		} else if ((unsigned)size > MAX_WORD_SIZE) {
			return luaL_error(L, "word_size must not exceed %d", MAX_WORD_SIZE);
		}
		options.size = (unsigned)size;
	}

	// Create userdata
	lucdb_wrap *wrap = lua_newuserdata(L, sizeof(lucdb_wrap));
	wrap->open = true;
	wrap->mode = mode;
	luaL_getmetatable(L, WRAP_NAME);
	lua_setmetatable(L, -2);

	// Open handle
	if (cdb_open(&wrap->handle, &options, mode, name) < 0) {
		wrap->open = false; // When cdb_open fails, cdb automatically closes the handle
		return err_ret(L, wrap);
	}

	lua_pushnil(L);
	return 2;
}

static inline int lucdb_reader_open(lua_State *L) {
	return create_wrap(L, CDB_RO_MODE);
}

static inline int lucdb_writer_open(lua_State *L) {
	return create_wrap(L, CDB_RW_MODE);
}

static int lucdb_type(lua_State *L) {
	if (lua_touserdata(L, 1) && lua_getmetatable(L, 1)) {
		luaL_getmetatable(L, WRAP_NAME);
		if (lua_rawequal(L, -1, -2)) {
			lua_pushstring(L, WRAP_NAME);
			return 1;
		}
	}

	lua_pushnil(L);
	return 1;
}


static int lucdb_wrap_close(lua_State *L) {
	lucdb_wrap *wrap = get_wrap(L, 1);
	safe_close_wrap(wrap);
	return 0;
}

static int lucdb_wrap_is_open(lua_State *L) {
	lua_pushboolean(L, get_wrap(L, 1)->open);
	return 1;
}

static int lucdb_wrap_get_mode(lua_State *L) {
	lua_pushstring(L, mode_name(get_wrap(L, 1)->mode));
	return 1;
}

static int lucdb_wrap_tostring(lua_State *L) {
	lucdb_wrap *wrap = get_wrap(L, 1);
	lua_pushfstring(L, "<"WRAP_NAME" %s %s>: %p", mode_name(wrap->mode), wrap->open ? "open" : "closed", wrap);
	return 1;
}


static int lucdb_wrap_count(lua_State *L) {
	lucdb_wrap *wrap = get_open_reader(L, 1);
	cdb_buffer_t key = buffer_from_lua_string(L, 2);

	uint64_t count;
	if (cdb_count(wrap->handle, &key, &count) < 0) {
		return err_ret(L, wrap);
	}

	lua_pushnumber(L, count);
	lua_pushnil(L);
	return 2;
}

static int lucdb_wrap_get(lua_State *L) {
	lucdb_wrap *wrap = get_open_reader(L, 1);
	cdb_buffer_t key = buffer_from_lua_string(L, 2);
    lua_Integer record = luaL_optinteger(L, 3, 1) - 1;
	luaL_argcheck(L, record >= 0, 3, "must be >= 1");

	cdb_file_pos_t location;
	int found = cdb_lookup(wrap->handle, &key, &location, (uint64_t)record);
	if (found == 0) {
		lua_pushboolean(L, false);
		lua_pushnil(L);
		return 2;
	} else if (found < 0 || !read_and_push(L, wrap->handle, &location)) {
		return err_ret(L, wrap);
	};

	lua_pushnil(L);
	return 2;
}

static int lucdb_wrap_get_all(lua_State *L) {
	lucdb_wrap *wrap = get_open_reader(L, 1);
	cdb_buffer_t key = buffer_from_lua_string(L, 2);

	uint64_t count;
	if (cdb_count(wrap->handle, &key, &count)) {
		return err_ret(L, wrap);
	}

	cdb_file_pos_t location;
	lua_createtable(L, count, 0);
	for (uint64_t i = 0; i < count; i++) {
		if (cdb_lookup(wrap->handle, &key, &location, i) <= 0 || !read_and_push(L, wrap->handle, &location)) {
			return err_ret(L, wrap);
		}
		lua_rawseti(L, -2, i + 1);
	}
	lua_pushnil(L);
	return 2;
}

static int lucdb_wrap_foreach_cb(cdb_t *handle, const cdb_file_pos_t *key, const cdb_file_pos_t *value, lua_State *L) {
	lua_pushvalue(L, 2);
	if (!read_and_push(L, handle, key) || !read_and_push(L, handle, value)) {
		return -1;
	}
	lua_call(L, 2, 1);  // Throwing an error here should be fine... cdb shouldn't be put into an invalid state

	if (lua_toboolean(L, -1)) {
		return 1;
	} else {
		lua_pop(L, 1);
		return 0;
	}
}

static int lucdb_wrap_foreach(lua_State *L) {
	lucdb_wrap *wrap = get_open_reader(L, 1);
	luaL_checktype(L, 2, LUA_TFUNCTION);

	if (cdb_foreach(wrap->handle, lucdb_wrap_foreach_cb, L) < 0) {
		return err_ret(L, wrap);
	}

	return ok_ret(L);
}


static int lucdb_wrap_add(lua_State *L) {
	lucdb_wrap *writer = get_open_writer(L, 1);
	cdb_buffer_t key   = buffer_from_lua_string(L, 2);
	cdb_buffer_t value = buffer_from_lua_string(L, 3);

	if (cdb_add(writer->handle, &key, &value) < 0) {
		return err_ret(L, writer);
	}

	return ok_ret(L);
}


static const struct luaL_Reg lucdb_wrap_meta[] = {
	{"__gc", lucdb_wrap_close},
	{"close", lucdb_wrap_close},
	{"is_open", lucdb_wrap_is_open},
	{"get_mode", lucdb_wrap_get_mode},
	{"__tostring", lucdb_wrap_tostring},

	{"count", lucdb_wrap_count},
	{"get", lucdb_wrap_get},
	{"get_all", lucdb_wrap_get_all},
	{"foreach", lucdb_wrap_foreach},

	{"add", lucdb_wrap_add},
	{NULL, NULL}
};

static const struct luaL_Reg lucdb_main[] = {
	{"reader", lucdb_reader_open},
	{"writer", lucdb_writer_open},
	{"type", lucdb_type},
	{NULL, NULL}
};

int luaopen_lucdb(lua_State *L) {
	luaL_newmetatable(L, WRAP_NAME);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, WRAP_NAME);
	lua_setfield(L, -2, "__name");
	lua_pushstring(L, WRAP_NAME);
	lua_setfield(L, -2, "__type");
	luaL_register(L, NULL, lucdb_wrap_meta);

	lua_newtable(L);
	luaL_register(L, NULL, lucdb_main);

	return 1;
}
