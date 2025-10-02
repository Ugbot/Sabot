#include <redismodule.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <jansson.h>

/* Module metadata */
#define MODULE_NAME "pgcache"
#define MODULE_VERSION 1

/* Global state */
typedef struct PGCtx {
    PGconn *pg_conn;
    char *pg_host;
    int pg_port;
    char *pg_database;
    char *pg_user;
    char *pg_password;
    int default_ttl;
    char *cache_prefix;
    RedisModuleCtx *redis_ctx;
    pthread_mutex_t conn_mutex;
} PGCtx;

static PGCtx *global_ctx = NULL;

/* Helper functions */
static PGconn* get_pg_connection(PGCtx *ctx) {
    pthread_mutex_lock(&ctx->conn_mutex);

    if (ctx->pg_conn == NULL || PQstatus(ctx->pg_conn) != CONNECTION_OK) {
        if (ctx->pg_conn) {
            PQfinish(ctx->pg_conn);
        }

        char conninfo[512];
        snprintf(conninfo, sizeof(conninfo),
                "host=%s port=%d dbname=%s user=%s password=%s",
                ctx->pg_host, ctx->pg_port, ctx->pg_database,
                ctx->pg_user, ctx->pg_password);

        ctx->pg_conn = PQconnectdb(conninfo);

        if (PQstatus(ctx->pg_conn) != CONNECTION_OK) {
            RedisModule_Log(ctx->redis_ctx, "warning",
                           "Failed to connect to PostgreSQL: %s",
                           PQerrorMessage(ctx->pg_conn));
            PQfinish(ctx->pg_conn);
            ctx->pg_conn = NULL;
        }
    }

    pthread_mutex_unlock(&ctx->conn_mutex);
    return ctx->pg_conn;
}

static char* build_cache_key(PGCtx *ctx, const char *table, const char *primary_key_json) {
    size_t key_len = strlen(ctx->cache_prefix) + strlen(table) + strlen(primary_key_json) + 3;
    char *cache_key = RedisModule_Alloc(key_len);
    snprintf(cache_key, key_len, "%s%s:%s", ctx->cache_prefix, table, primary_key_json);
    return cache_key;
}

static void publish_event(PGCtx *ctx, const char *event_type, const char *table, const char *data) {
    RedisModuleCtx *rctx = ctx->redis_ctx;

    // Create event JSON
    json_t *event = json_object();
    json_object_set_new(event, "type", json_string(event_type));
    json_object_set_new(event, "table", json_string(table));
    json_object_set_new(event, "timestamp", json_real((double)time(NULL)));

    if (data) {
        json_object_set_new(event, "data", json_string(data));
    }

    char *event_str = json_dumps(event, JSON_COMPACT);

    // Publish to Redis pubsub
    RedisModule_Call(rctx, "PUBLISH", "cc", "pg_cache_events", event_str);

    free(event_str);
    json_decref(event);
}

static json_t* execute_pg_query(PGCtx *ctx, const char *query, int *row_count) {
    PGconn *conn = get_pg_connection(ctx);
    if (!conn) {
        return NULL;
    }

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        RedisModule_Log(ctx->redis_ctx, "warning",
                       "PostgreSQL query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return NULL;
    }

    int ntuples = PQntuples(res);
    int nfields = PQnfields(res);
    *row_count = ntuples;

    json_t *result_array = json_array();

    for (int i = 0; i < ntuples; i++) {
        json_t *row_obj = json_object();

        for (int j = 0; j < nfields; j++) {
            const char *colname = PQfname(res, j);
            const char *value = PQgetvalue(res, i, j);

            if (PQgetisnull(res, i, j)) {
                json_object_set_new(row_obj, colname, json_null());
            } else {
                json_object_set_new(row_obj, colname, json_string(value));
            }
        }

        json_array_append_new(result_array, row_obj);
    }

    PQclear(res);
    return result_array;
}

/* Redis commands */

/* PGCACHE.READ <table> <primary_key_json> [TTL] */
int PGCRead_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_key_json = RedisModule_StringPtrLen(argv[2], NULL);
    int ttl = (argc == 4) ? atoi(RedisModule_StringPtrLen(argv[3], NULL)) : pgctx->default_ttl;

    // Build cache key
    char *cache_key = build_cache_key(pgctx, table, primary_key_json);

    // Try to get from cache first
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "c", cache_key);

    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
        // Cache hit
        size_t len;
        const char *cached_data = RedisModule_CallReplyStringPtr(reply, &len);

        publish_event(pgctx, "cache_hit", table, cached_data);

        // Update access time for LRU (optional)
        RedisModule_Call(ctx, "HINCRBY", "ccc", cache_key, "hits", "1");

        RedisModule_ReplyWithStringBuffer(ctx, cached_data, len);
        RedisModule_FreeCallReply(reply);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }

    RedisModule_FreeCallReply(reply);

    // Cache miss - query PostgreSQL
    // Build query from primary key JSON
    json_t *pk_data = json_loads(primary_key_json, 0, NULL);
    if (!pk_data || !json_is_object(pk_data)) {
        RedisModule_ReplyWithError(ctx, "Invalid primary key JSON");
        if (pk_data) json_decref(pk_data);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }

    // Build WHERE clause
    const char *key;
    json_t *value;
    char where_clause[2048] = "";
    int first = 1;

    json_object_foreach(pk_data, key, value) {
        if (!first) {
            strncat(where_clause, " AND ", sizeof(where_clause) - strlen(where_clause) - 1);
        }
        char condition[256];
        if (json_is_string(value)) {
            snprintf(condition, sizeof(condition), "%s = '%s'",
                    key, json_string_value(value));
        } else {
            snprintf(condition, sizeof(condition), "%s = %s",
                    key, json_string_value(value));
        }
        strncat(where_clause, condition, sizeof(where_clause) - strlen(where_clause) - 1);
        first = 0;
    }

    char query[4096];
    snprintf(query, sizeof(query), "SELECT * FROM %s WHERE %s", table, where_clause);

    json_decref(pk_data);

    int row_count = 0;
    json_t *pg_result = execute_pg_query(pgctx, query, &row_count);

    if (!pg_result || json_array_size(pg_result) == 0) {
        // No data found
        publish_event(pgctx, "cache_miss", table, NULL);
        RedisModule_ReplyWithNull(ctx);
        if (pg_result) json_decref(pg_result);
        RedisModule_Free(cache_key);
        return REDISMODULE_OK;
    }

    // Get first row and cache it
    json_t *row_data = json_array_get(pg_result, 0);
    char *data_str = json_dumps(row_data, JSON_COMPACT);

    // Store in Redis cache
    RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl, data_str);

    // Update miss statistics
    RedisModule_Call(ctx, "HINCRBY", "ccc", cache_key, "misses", "1");

    publish_event(pgctx, "cache_miss", table, data_str);

    RedisModule_ReplyWithStringBuffer(ctx, data_str, strlen(data_str));

    free(data_str);
    json_decref(pg_result);
    RedisModule_Free(cache_key);

    return REDISMODULE_OK;
}

/* PGCACHE.WRITE <table> <primary_key_json> <data_json> [TTL] */
int PGCWrite_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || argc > 5) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_key_json = RedisModule_StringPtrLen(argv[2], NULL);
    const char *data_json = RedisModule_StringPtrLen(argv[3], NULL);
    int ttl = (argc == 5) ? atoi(RedisModule_StringPtrLen(argv[4], NULL)) : pgctx->default_ttl;

    // Build cache key
    char *cache_key = build_cache_key(pgctx, table, primary_key_json);

    // Store in Redis cache
    RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl, data_json);

    publish_event(pgctx, "cache_write", table, data_json);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_Free(cache_key);

    return REDISMODULE_OK;
}

/* PGCACHE.INVALIDATE <table> <primary_key_json> */
int PGCInvalidate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_key_json = RedisModule_StringPtrLen(argv[2], NULL);

    // Build cache key
    char *cache_key = build_cache_key(pgctx, table, primary_key_json);

    // Delete from cache
    RedisModule_Call(ctx, "DEL", "c", cache_key);

    publish_event(pgctx, "cache_invalidate", table, primary_key_json);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_Free(cache_key);

    return REDISMODULE_OK;
}

/* PGCACHE.MULTIREAD <table> <primary_keys_json_array> [TTL] */
int PGCMultiRead_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3 || argc > 4) {
        return RedisModule_WrongArity(ctx);
    }

    PGCtx *pgctx = global_ctx;
    if (!pgctx) {
        return RedisModule_ReplyWithError(ctx, "PostgreSQL cache not initialized");
    }

    const char *table = RedisModule_StringPtrLen(argv[1], NULL);
    const char *primary_keys_json = RedisModule_StringPtrLen(argv[2], NULL);
    int ttl = (argc == 4) ? atoi(RedisModule_StringPtrLen(argv[3], NULL)) : pgctx->default_ttl;

    // Parse primary keys array
    json_t *pk_array = json_loads(primary_keys_json, 0, NULL);
    if (!pk_array || !json_is_array(pk_array)) {
        RedisModule_ReplyWithError(ctx, "Invalid primary keys JSON array");
        if (pk_array) json_decref(pk_array);
        return REDISMODULE_OK;
    }

    json_t *result_obj = json_object();
    int pk_count = json_array_size(pk_array);

    // Check cache for all keys first
    char **cache_keys = RedisModule_Alloc(sizeof(char*) * pk_count);
    int *cache_hits = RedisModule_Alloc(sizeof(int) * pk_count);
    const char **cached_values = RedisModule_Alloc(sizeof(char*) * pk_count);

    for (int i = 0; i < pk_count; i++) {
        json_t *pk_obj = json_array_get(pk_array, i);
        char *pk_json = json_dumps(pk_obj, JSON_COMPACT);
        cache_keys[i] = build_cache_key(pgctx, table, pk_json);

        RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "c", cache_keys[i]);
        if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
            size_t len;
            cached_values[i] = RedisModule_CallReplyStringPtr(reply, &len);
            cache_hits[i] = 1;

            // Copy the string since the reply will be freed
            char *value_copy = RedisModule_Alloc(len + 1);
            memcpy(value_copy, cached_values[i], len);
            value_copy[len] = '\0';
            cached_values[i] = value_copy;
        } else {
            cache_hits[i] = 0;
            cached_values[i] = NULL;
        }
        RedisModule_FreeCallReply(reply);
        free(pk_json);
    }

    // Build query for cache misses
    char query[8192] = "";
    int miss_count = 0;

    for (int i = 0; i < pk_count; i++) {
        if (!cache_hits[i]) {
            json_t *pk_obj = json_array_get(pk_array, i);
            char *pk_json = json_dumps(pk_obj, JSON_COMPACT);

            if (miss_count > 0) {
                strncat(query, " OR ", sizeof(query) - strlen(query) - 1);
            }

            // Build OR condition for this primary key
            const char *key;
            json_t *value;
            char condition[1024] = "(";
            int first = 1;

            json_object_foreach(pk_obj, key, value) {
                if (!first) {
                    strncat(condition, " AND ", sizeof(condition) - strlen(condition) - 1);
                }
                char part[256];
                if (json_is_string(value)) {
                    snprintf(part, sizeof(part), "%s = '%s'", key, json_string_value(value));
                } else {
                    snprintf(part, sizeof(part), "%s = %s", key, json_string_value(value));
                }
                strncat(condition, part, sizeof(condition) - strlen(condition) - 1);
                first = 0;
            }
            strncat(condition, ")", sizeof(condition) - strlen(condition) - 1);

            strncat(query, condition, sizeof(query) - strlen(query) - 1);
            miss_count++;
            free(pk_json);
        }
    }

    if (miss_count > 0) {
        char full_query[16384];
        snprintf(full_query, sizeof(full_query), "SELECT * FROM %s WHERE %s", table, query);

        int row_count = 0;
        json_t *pg_result = execute_pg_query(pgctx, full_query, &row_count);

        if (pg_result && json_array_size(pg_result) > 0) {
            // Cache the results
            for (size_t i = 0; i < json_array_size(pg_result); i++) {
                json_t *row = json_array_get(pg_result, i);
                char *row_json = json_dumps(row, JSON_COMPACT);

                // Find corresponding primary key to build cache key
                // This is simplified - in practice you'd need to extract PK from row
                char cache_key[1024];
                snprintf(cache_key, sizeof(cache_key), "%s%s:row_%zu", pgctx->cache_prefix, table, i);

                RedisModule_Call(ctx, "SETEX", "ccc", cache_key, ttl, row_json);
                free(row_json);
            }
        }

        if (pg_result) json_decref(pg_result);
    }

    // Build response
    json_t *response_array = json_array();

    for (int i = 0; i < pk_count; i++) {
        if (cache_hits[i]) {
            json_t *cached_obj = json_loads(cached_values[i], 0, NULL);
            if (cached_obj) {
                json_array_append_new(response_array, cached_obj);
            }
            RedisModule_Free((char*)cached_values[i]);
        } else {
            // For misses, we return null (could be enhanced to return from DB)
            json_array_append_new(response_array, json_null());
        }
    }

    char *response_str = json_dumps(response_array, JSON_COMPACT);
    RedisModule_ReplyWithStringBuffer(ctx, response_str, strlen(response_str));

    free(response_str);
    json_decref(response_array);
    json_decref(pk_array);

    for (int i = 0; i < pk_count; i++) {
        RedisModule_Free(cache_keys[i]);
    }
    RedisModule_Free(cache_keys);
    RedisModule_Free(cache_hits);
    RedisModule_Free(cached_values);

    return REDISMODULE_OK;
}

/* Module initialization */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, MODULE_NAME, MODULE_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Initialize global context
    global_ctx = RedisModule_Alloc(sizeof(PGCtx));
    if (!global_ctx) {
        return REDISMODULE_ERR;
    }

    memset(global_ctx, 0, sizeof(PGCtx));
    global_ctx->redis_ctx = ctx;
    global_ctx->default_ttl = 3600;
    global_ctx->cache_prefix = RedisModule_Strdup("pg_cache:");

    pthread_mutex_init(&global_ctx->conn_mutex, NULL);

    // Parse module arguments
    for (int i = 0; i < argc; i += 2) {
        const char *param = RedisModule_StringPtrLen(argv[i], NULL);
        const char *value = RedisModule_StringPtrLen(argv[i + 1], NULL);

        if (strcmp(param, "pg_host") == 0) {
            global_ctx->pg_host = RedisModule_Strdup(value);
        } else if (strcmp(param, "pg_port") == 0) {
            global_ctx->pg_port = atoi(value);
        } else if (strcmp(param, "pg_database") == 0) {
            global_ctx->pg_database = RedisModule_Strdup(value);
        } else if (strcmp(param, "pg_user") == 0) {
            global_ctx->pg_user = RedisModule_Strdup(value);
        } else if (strcmp(param, "pg_password") == 0) {
            global_ctx->pg_password = RedisModule_Strdup(value);
        } else if (strcmp(param, "default_ttl") == 0) {
            global_ctx->default_ttl = atoi(value);
        } else if (strcmp(param, "cache_prefix") == 0) {
            RedisModule_Free(global_ctx->cache_prefix);
            global_ctx->cache_prefix = RedisModule_Strdup(value);
        }
    }

    // Set defaults if not provided
    if (!global_ctx->pg_host) global_ctx->pg_host = RedisModule_Strdup("localhost");
    if (!global_ctx->pg_database) global_ctx->pg_database = RedisModule_Strdup("postgres");
    if (!global_ctx->pg_user) global_ctx->pg_user = RedisModule_Strdup("postgres");
    if (!global_ctx->pg_password) global_ctx->pg_password = RedisModule_Strdup("");

    // Register commands
    if (RedisModule_CreateCommand(ctx, "pgcache.read", PGCRead_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.write", PGCWrite_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.invalidate", PGCInvalidate_RedisCommand,
                                 "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "pgcache.multiread", PGCMultiRead_RedisCommand,
                                 "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx, "notice", "PostgreSQL cache Redis module loaded");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    if (global_ctx) {
        if (global_ctx->pg_conn) {
            PQfinish(global_ctx->pg_conn);
        }

        RedisModule_Free(global_ctx->pg_host);
        RedisModule_Free(global_ctx->pg_database);
        RedisModule_Free(global_ctx->pg_user);
        RedisModule_Free(global_ctx->pg_password);
        RedisModule_Free(global_ctx->cache_prefix);

        pthread_mutex_destroy(&global_ctx->conn_mutex);
        RedisModule_Free(global_ctx);
    }

    return REDISMODULE_OK;
}
