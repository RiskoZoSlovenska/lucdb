#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "cdb/cdb.h"
#include "cdb/host.h"

#define READER_NAME "lucdb.reader"
#define WRITER_NAME "lucdb.writer"


struct lucdb_wrapper {
	cdb_t *handle;
	bool open;
};

typedef struct lucdb_wrapper lucdb_wrapper;
typedef lucdb_wrapper lucdb_reader;
typedef lucdb_wrapper lucdb_writer;


static inline lucdb_reader* get_reader(lua_State *L, int index) {
	return (lucdb_reader *)luaL_checkudata(L, index, READER_NAME);
}

static inline lucdb_writer* get_writer(lua_State *L, int index) {
	return (lucdb_writer *)luaL_checkudata(L, index, WRITER_NAME);
}

static inline lucdb_wrapper* check_not_closed(lua_State *L, lucdb_wrapper *obj) {
	if (!obj->open) {
		luaL_error(L, "handle has been closed");
	}
	return obj;
}


static inline int err_ret(lua_State *L, lucdb_wrapper *obj) {
	lua_pushnil(L);
	lua_pushnumber(L, cdb_status(obj->handle));
	cdb_close(obj->handle);
	return 2;
}

static inline int ok_ret(lua_State *L) {
	lua_pushboolean(L, true);
	lua_pushnil(L);
	return 2;
}


static int lucdb_open(lua_State *L, const cdb_options_t *opts, int mode, const char *meta_name) {
	size_t name_len;
	const char *name = luaL_checklstring(L, 1, &name_len);
	if (strlen(name) < name_len) {
		luaL_error(L, "filename must not contain zero characters");
	}

	lucdb_wrapper *reader = lua_newuserdata(L, sizeof(lucdb_wrapper));
	reader->open = true;
	luaL_getmetatable(L, meta_name);
	lua_setmetatable(L, -2);

	if (cdb_open(&reader->handle, opts, mode, name) < 0) {
		reader->open = false;
		return err_ret(L, reader);
	}

	lua_pushnil(L);
	return 2;
}

static int lucdb_reader_open(lua_State *L) {
	return lucdb_open(L, &cdb_host_options, CDB_RO_MODE, READER_NAME);
}

static int lucdb_writer_open(lua_State *L) {
	return lucdb_open(L, &cdb_host_options, CDB_RW_MODE, WRITER_NAME);
}


static int lucdb_reader_close(lua_State *L) {
	lucdb_reader *reader = get_reader(L, 1);
	if (reader->open) {
		cdb_close(reader->handle);
		reader->open = false;
	}
	return 0;
}

static int lucdb_writer_close(lua_State *L) {
	lucdb_writer *writer = get_writer(L, 1);
	if (writer->open) {
		cdb_close(writer->handle);
		writer->open = false;
	}
	return 0;
}


static int lucdb_reader_tostring(lua_State *L) {
	lucdb_reader *reader = get_reader(L, 1);
	lua_pushfstring(L, "<"READER_NAME"> (%s)", reader->open ? "open" : "closed");
	return 1;
}

static int lucdb_writer_tostring(lua_State *L) {
	lucdb_wrapper *writer = get_writer(L, 1);
	lua_pushfstring(L, "<"WRITER_NAME"> (%s)", writer->open ? "open" : "closed");
	return 1;
}


static cdb_buffer_t buffer_from_lua_string(lua_State *L, int index) {
	size_t len;
	const char* str = luaL_checklstring(L, index, &len);
	cdb_buffer_t value = { .buffer = str, .length = len };
	return value;
}

static int lucdb_reader_count(lua_State *L) {
	lucdb_reader *reader = check_not_closed(L, get_reader(L, 1));

	cdb_buffer_t key = buffer_from_lua_string(L, 2);
	uint64_t count;
	if (cdb_count(reader->handle, &key, &count) < 0) {
		return err_ret(L, reader);
	}

	lua_pushnumber(L, count);
	lua_pushnil(L);
	return 2;
}

static void* read_location(cdb_t *handle, const cdb_file_pos_t *location) {
	void *data = malloc(location->length);

	if (cdb_seek(handle, location->position) < 0 || cdb_read(handle, data, location->length) < 0) {
		return NULL;
	}

	return data;
}

static int read_and_push(lua_State *L, cdb_t *handle, const cdb_file_pos_t *location) {
	void* data = read_location(handle, location);
	if (!data) {
		return 0;
	}
	lua_pushlstring(L, data, location->length);
	free(data);
	return 1;
}

static int lucdb_reader_get(lua_State *L) {
	lucdb_reader *reader = check_not_closed(L, get_reader(L, 1));

	cdb_buffer_t key = buffer_from_lua_string(L, 2);
    cdb_file_pos_t location;
    lua_Integer record = luaL_optinteger(L, 3, 1) - 1;
	luaL_argcheck(L, record >= 0, 3, "must be >= 1");

	int found = cdb_lookup(reader->handle, &key, &location, (uint64_t)record);
	if (found == 0) {
		lua_pushboolean(L, false);
		lua_pushnil(L);
		return 2;
	} else if (found < 0 || !read_and_push(L, reader->handle, &location)) {
		return err_ret(L, reader);
	};

	lua_pushnil(L);
	return 2;
}

static int lucdb_reader_get_all(lua_State *L) {
	lucdb_reader *reader = check_not_closed(L, get_reader(L, 1));

	cdb_buffer_t key = buffer_from_lua_string(L, 2);
	uint64_t count;
	if (cdb_count(reader->handle, &key, &count)) {
		return err_ret(L, reader);
	}

	cdb_file_pos_t location;
	lua_createtable(L, count, 0);
	for (uint64_t i = 0; i < count; i++) {
		if (cdb_lookup(reader->handle, &key, &location, i) <= 0 || !read_and_push(L, reader->handle, &location)) {
			return err_ret(L, reader);
		}
		lua_rawseti(L, -2, i + 1);
	}
	lua_pushnil(L);
	return 2;
}

static int lucdb_reader_foreach_cb(cdb_t *handle, const cdb_file_pos_t *key, const cdb_file_pos_t *value, lua_State *L) {
	lua_pushvalue(L, 2);

	if (!read_and_push(L, handle, key) || !read_and_push(L, handle, value)) {
		return -1;
	}
	lua_call(L, 2, 1);  // Throwing an error here should be fine... cdb shouldn't be put into an invalid state

	if (lua_toboolean(L, -1)) {
		return 1;
	}
	lua_pop(L, 1);
	return 0;
}

static int lucdb_reader_foreach(lua_State *L) {
	lucdb_reader *reader = check_not_closed(L, get_reader(L, 1));
	luaL_checktype(L, 2, LUA_TFUNCTION);

	if (cdb_foreach(reader->handle, lucdb_reader_foreach_cb, L) < 0) {
		return err_ret(L, reader);
	}

	return ok_ret(L);
}

static int lucdb_writer_insert(lua_State *L) {
	lucdb_writer *writer = check_not_closed(L, get_writer(L, 1));

	cdb_buffer_t key   = buffer_from_lua_string(L, 2);
	cdb_buffer_t value = buffer_from_lua_string(L, 3);

	if (cdb_add(writer->handle, &key, &value) < 0) {
		return err_ret(L, writer);
	}

	return ok_ret(L);
}


static const struct luaL_Reg lucdb_reader_meta[] = {
	{"__gc", lucdb_reader_close},
	{"close", lucdb_reader_close},
	{"__tostring", lucdb_reader_tostring},
	{"count", lucdb_reader_count},
	{"get", lucdb_reader_get},
	{"get_all", lucdb_reader_get_all},
	{"foreach", lucdb_reader_foreach},
	{NULL, NULL}
};

static const struct luaL_Reg lucdb_writer_meta[] = {
	{"__gc", lucdb_writer_close},
	{"close", lucdb_writer_close},
	{"__tostring", lucdb_writer_tostring},
	{"insert", lucdb_writer_insert},
	{NULL, NULL}
};

static const struct luaL_Reg lucdb_main[] = {
	{"reader", lucdb_reader_open},
	{"writer", lucdb_writer_open},
	{NULL, NULL}
};

int luaopen_lucdb(lua_State *L) {
	luaL_newmetatable(L, READER_NAME);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, READER_NAME);
	lua_setfield(L, -2, "__name");
	luaL_register(L, NULL, lucdb_reader_meta);

	luaL_newmetatable(L, WRITER_NAME);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, WRITER_NAME);
	lua_setfield(L, -2, "__name");
	luaL_register(L, NULL, lucdb_writer_meta);

	lua_newtable(L);
	luaL_register(L, NULL, lucdb_main);

	return 1;
}
