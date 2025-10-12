#include "sabot_sql/main/capi/capi_internal.hpp"

using sabot_sql::CClientArrowOptionsWrapper;
using sabot_sql::CClientContextWrapper;
using sabot_sql::Connection;
using sabot_sql::DatabaseWrapper;
using sabot_sql::DBConfig;
using sabot_sql::DBInstanceCacheWrapper;
using sabot_sql::SabotSQL;
using sabot_sql::ErrorData;

sabot_sql_instance_cache sabot_sql_create_instance_cache() {
	auto wrapper = new DBInstanceCacheWrapper();
	wrapper->instance_cache = sabot_sql::make_uniq<sabot_sql::DBInstanceCache>();
	return reinterpret_cast<sabot_sql_instance_cache>(wrapper);
}

void sabot_sql_destroy_instance_cache(sabot_sql_instance_cache *instance_cache) {
	if (instance_cache && *instance_cache) {
		auto wrapper = reinterpret_cast<DBInstanceCacheWrapper *>(*instance_cache);
		delete wrapper;
		*instance_cache = nullptr;
	}
}

sabot_sql_state sabot_sql_open_internal(DBInstanceCacheWrapper *cache, const char *path, sabot_sql_database *out,
                                  sabot_sql_config config, char **out_error) {
	auto wrapper = new DatabaseWrapper();
	try {
		DBConfig default_config;
		default_config.SetOptionByName("sabot_sql_api", "capi");

		DBConfig *db_config = &default_config;
		DBConfig *user_config = reinterpret_cast<DBConfig *>(config);
		if (user_config) {
			db_config = user_config;
		}

		if (cache) {
			sabot_sql::string path_str;
			if (path) {
				path_str = path;
			}
			wrapper->database = cache->instance_cache->GetOrCreateInstance(path_str, *db_config, true);
		} else {
			wrapper->database = sabot_sql::make_shared_ptr<SabotSQL>(path, db_config);
		}

	} catch (std::exception &ex) {
		if (out_error) {
			ErrorData parsed_error(ex);
			*out_error = strdup(parsed_error.Message().c_str());
		}
		delete wrapper;
		return SabotSQLError;

	} catch (...) { // LCOV_EXCL_START
		if (out_error) {
			*out_error = strdup("Unknown error");
		}
		delete wrapper;
		return SabotSQLError;
	} // LCOV_EXCL_STOP

	*out = reinterpret_cast<sabot_sql_database>(wrapper);
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_get_or_create_from_cache(sabot_sql_instance_cache instance_cache, const char *path,
                                             sabot_sql_database *out_database, sabot_sql_config config, char **out_error) {
	if (!instance_cache) {
		if (out_error) {
			*out_error = strdup("instance cache cannot be nullptr");
		}
		return SabotSQLError;
	}
	auto cache = reinterpret_cast<DBInstanceCacheWrapper *>(instance_cache);
	return sabot_sql_open_internal(cache, path, out_database, config, out_error);
}

sabot_sql_state sabot_sql_open_ext(const char *path, sabot_sql_database *out, sabot_sql_config config, char **error) {
	return sabot_sql_open_internal(nullptr, path, out, config, error);
}

sabot_sql_state sabot_sql_open(const char *path, sabot_sql_database *out) {
	return sabot_sql_open_ext(path, out, nullptr, nullptr);
}

void sabot_sql_close(sabot_sql_database *database) {
	if (database && *database) {
		auto wrapper = reinterpret_cast<DatabaseWrapper *>(*database);
		delete wrapper;
		*database = nullptr;
	}
}

sabot_sql_state sabot_sql_connect(sabot_sql_database database, sabot_sql_connection *out) {
	if (!database || !out) {
		return SabotSQLError;
	}

	auto wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	Connection *connection;
	try {
		connection = new Connection(*wrapper->database);
	} catch (...) { // LCOV_EXCL_START
		return SabotSQLError;
	} // LCOV_EXCL_STOP

	*out = reinterpret_cast<sabot_sql_connection>(connection);
	return SabotSQLSuccess;
}

void sabot_sql_interrupt(sabot_sql_connection connection) {
	if (!connection) {
		return;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	conn->Interrupt();
}

sabot_sql_query_progress_type sabot_sql_query_progress(sabot_sql_connection connection) {
	sabot_sql_query_progress_type query_progress_type;
	query_progress_type.percentage = -1;
	query_progress_type.total_rows_to_process = 0;
	query_progress_type.rows_processed = 0;
	if (!connection) {
		return query_progress_type;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto query_progress = conn->context->GetQueryProgress();
	query_progress_type.total_rows_to_process = query_progress.GetTotalRowsToProcess();
	query_progress_type.rows_processed = query_progress.GetRowsProcesseed();
	query_progress_type.percentage = query_progress.GetPercentage();
	return query_progress_type;
}

void sabot_sql_disconnect(sabot_sql_connection *connection) {
	if (connection && *connection) {
		Connection *conn = reinterpret_cast<Connection *>(*connection);
		delete conn;
		*connection = nullptr;
	}
}

void sabot_sql_connection_get_client_context(sabot_sql_connection connection, sabot_sql_client_context *out_context) {
	if (!connection || !out_context) {
		return;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		auto wrapper = new CClientContextWrapper(*conn->context);
		*out_context = reinterpret_cast<sabot_sql_client_context>(wrapper);
	} catch (...) {
		*out_context = nullptr;
	}
}

void sabot_sql_connection_get_arrow_options(sabot_sql_connection connection, sabot_sql_arrow_options *out_arrow_options) {
	if (!connection || !out_arrow_options) {
		return;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		auto client_properties = conn->context->GetClientProperties();
		auto wrapper = new CClientArrowOptionsWrapper(client_properties);
		*out_arrow_options = reinterpret_cast<sabot_sql_arrow_options>(wrapper);
	} catch (...) {
		*out_arrow_options = nullptr;
	}
}

idx_t sabot_sql_client_context_get_connection_id(sabot_sql_client_context context) {
	auto wrapper = reinterpret_cast<CClientContextWrapper *>(context);
	return wrapper->context.GetConnectionId();
}

void sabot_sql_destroy_client_context(sabot_sql_client_context *context) {
	if (context && *context) {
		auto wrapper = reinterpret_cast<CClientContextWrapper *>(*context);
		delete wrapper;
		*context = nullptr;
	}
}

void sabot_sql_destroy_arrow_options(sabot_sql_arrow_options *arrow_options) {
	if (arrow_options && *arrow_options) {
		auto wrapper = reinterpret_cast<CClientArrowOptionsWrapper *>(*arrow_options);
		delete wrapper;
		*arrow_options = nullptr;
	}
}

sabot_sql_state sabot_sql_query(sabot_sql_connection connection, const char *query, sabot_sql_result *out) {
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		auto result = conn->Query(query);
		return SabotSQLTranslateResult(std::move(result), out);
	} catch (...) {
		return SabotSQLError;
	}
}

const char *sabot_sql_library_version() {
	return SabotSQL::LibraryVersion();
}

sabot_sql_value sabot_sql_get_table_names(sabot_sql_connection connection, const char *query, bool qualified) {
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		auto table_names = conn->GetTableNames(query, qualified);

		auto count = table_names.size();
		auto ptr = malloc(count * sizeof(sabot_sql_value));
		if (!ptr) {
			return nullptr;
		}
		auto list_values = reinterpret_cast<sabot_sql_value *>(ptr);

		try {
			idx_t name_ix = 0;
			for (const auto &name : table_names) {
				list_values[name_ix] = sabot_sql_create_varchar(name.c_str());
				name_ix++;
			}

			auto varchar_type = sabot_sql_create_logical_type(SABOT_SQL_TYPE_VARCHAR);
			auto list_value = sabot_sql_create_list_value(varchar_type, list_values, count);

			for (idx_t i = 0; i < count; i++) {
				sabot_sql_destroy_value(&list_values[i]);
			}
			sabot_sql_free(ptr);
			sabot_sql_destroy_logical_type(&varchar_type);

			return list_value;
		} catch (...) {
			sabot_sql_free(ptr);
			return nullptr;
		}
	} catch (...) {
		return nullptr;
	}
}
