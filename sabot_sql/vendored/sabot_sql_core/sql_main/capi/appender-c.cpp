#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/uhugeint.hpp"

using sabot_sql::Appender;
using sabot_sql::AppenderWrapper;
using sabot_sql::BaseAppender;
using sabot_sql::Connection;
using sabot_sql::date_t;
using sabot_sql::dtime_t;
using sabot_sql::ErrorData;
using sabot_sql::ErrorDataWrapper;
using sabot_sql::hugeint_t;
using sabot_sql::interval_t;
using sabot_sql::string_t;
using sabot_sql::timestamp_t;
using sabot_sql::uhugeint_t;

sabot_sql_state sabot_sql_appender_create(sabot_sql_connection connection, const char *schema, const char *table,
                                    sabot_sql_appender *out_appender) {
	return sabot_sql_appender_create_ext(connection, INVALID_CATALOG, schema, table, out_appender);
}

sabot_sql_state sabot_sql_appender_create_ext(sabot_sql_connection connection, const char *catalog, const char *schema,
                                        const char *table, sabot_sql_appender *out_appender) {
	Connection *conn = reinterpret_cast<Connection *>(connection);

	if (!connection || !table || !out_appender) {
		return SabotSQLError;
	}
	if (catalog == nullptr) {
		catalog = INVALID_CATALOG;
	}
	if (schema == nullptr) {
		schema = DEFAULT_SCHEMA;
	}

	auto wrapper = new AppenderWrapper();
	*out_appender = reinterpret_cast<sabot_sql_appender>(wrapper);
	try {
		wrapper->appender = sabot_sql::make_uniq<Appender>(*conn, catalog, schema, table);
	} catch (std::exception &ex) {
		wrapper->error_data = ErrorData(ex);
		return SabotSQLError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error_data = ErrorData("Unknown create appender error");
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_appender_create_query(sabot_sql_connection connection, const char *query, idx_t column_count,
                                          sabot_sql_logical_type *types_p, const char *table_name_p,
                                          const char **column_names_p, sabot_sql_appender *out_appender) {
	Connection *conn = reinterpret_cast<Connection *>(connection);

	if (!connection || !query || !column_count || !types_p) {
		return SabotSQLError;
	}
	sabot_sql::vector<sabot_sql::LogicalType> types;
	sabot_sql::vector<sabot_sql::string> column_names;
	sabot_sql::string table_name;
	for (idx_t c = 0; c < column_count; ++c) {
		if (!types_p[c]) {
			return SabotSQLError;
		}
		types.push_back(*reinterpret_cast<sabot_sql::LogicalType *>(types_p[c]));
	}
	if (table_name_p) {
		table_name = table_name_p;
	}
	if (column_names_p) {
		for (idx_t c = 0; c < column_count; ++c) {
			if (!column_names_p[c]) {
				return SabotSQLError;
			}
			column_names.push_back(column_names_p[c]);
		}
	}

	auto wrapper = new AppenderWrapper();
	*out_appender = reinterpret_cast<sabot_sql_appender>(wrapper);
	try {
		wrapper->appender = sabot_sql::make_uniq<sabot_sql::QueryAppender>(*conn, query, std::move(types),
		                                                             std::move(column_names), std::move(table_name));
	} catch (std::exception &ex) {
		wrapper->error_data = ErrorData(ex);
		return SabotSQLError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error_data = ErrorData("Unknown create appender error");
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_appender_destroy(sabot_sql_appender *appender) {
	if (!appender || !*appender) {
		return SabotSQLError;
	}
	auto state = sabot_sql_appender_close(*appender);
	auto wrapper = reinterpret_cast<AppenderWrapper *>(*appender);
	if (wrapper) {
		delete wrapper;
	}
	*appender = nullptr;
	return state;
}

template <class FUN>
sabot_sql_state sabot_sql_appender_run_function(sabot_sql_appender appender, FUN &&function) {
	if (!appender) {
		return SabotSQLError;
	}
	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		wrapper->error_data = ErrorData("not a valid appender");
		return SabotSQLError;
	}
	try {
		function(*wrapper->appender);
	} catch (std::exception &ex) {
		wrapper->error_data = ErrorData(ex);
		return SabotSQLError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error_data = ErrorData("Unknown appender error");
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_appender_add_column(sabot_sql_appender appender_p, const char *name) {
	return sabot_sql_appender_run_function(appender_p, [&](BaseAppender &appender) { appender.AddColumn(name); });
}

sabot_sql_state sabot_sql_appender_clear_columns(sabot_sql_appender appender_p) {
	return sabot_sql_appender_run_function(appender_p, [&](BaseAppender &appender) { appender.ClearColumns(); });
}

const char *sabot_sql_appender_error(sabot_sql_appender appender) {
	if (!appender) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->error_data.HasError()) {
		return nullptr;
	}
	return wrapper->error_data.RawMessage().c_str();
}

sabot_sql_error_data sabot_sql_appender_error_data(sabot_sql_appender appender) {
	auto errorDataWrapper = new ErrorDataWrapper();
	if (!appender) {
		return reinterpret_cast<sabot_sql_error_data>(errorDataWrapper);
	}

	auto appenderWrapper = reinterpret_cast<AppenderWrapper *>(appender);
	errorDataWrapper->error_data = appenderWrapper->error_data;
	return reinterpret_cast<sabot_sql_error_data>(errorDataWrapper);
}

sabot_sql_state sabot_sql_appender_begin_row(sabot_sql_appender appender) {
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_appender_end_row(sabot_sql_appender appender_p) {
	return sabot_sql_appender_run_function(appender_p, [&](BaseAppender &appender) { appender.EndRow(); });
}

template <class T>
sabot_sql_state sabot_sql_append_internal(sabot_sql_appender appender, T value) {
	if (!appender) {
		return SabotSQLError;
	}
	auto *appender_instance = reinterpret_cast<AppenderWrapper *>(appender);
	try {
		appender_instance->appender->Append<T>(value);
	} catch (std::exception &ex) {
		appender_instance->error_data = ErrorData(ex);
		return SabotSQLError;
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_append_default(sabot_sql_appender appender) {
	if (!appender) {
		return SabotSQLError;
	}
	auto *appender_instance = reinterpret_cast<AppenderWrapper *>(appender);

	try {
		appender_instance->appender->AppendDefault();
	} catch (std::exception &ex) {
		appender_instance->error_data = ErrorData(ex);
		return SabotSQLError;
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_append_default_to_chunk(sabot_sql_appender appender, sabot_sql_data_chunk chunk, idx_t col, idx_t row) {
	if (!appender || !chunk) {
		return SabotSQLError;
	}

	auto *appender_instance = reinterpret_cast<AppenderWrapper *>(appender);

	auto data_chunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);

	try {
		appender_instance->appender->AppendDefault(*data_chunk, col, row);
	} catch (std::exception &ex) {
		appender_instance->error_data = ErrorData(ex);
		return SabotSQLError;
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_append_bool(sabot_sql_appender appender, bool value) {
	return sabot_sql_append_internal<bool>(appender, value);
}

sabot_sql_state sabot_sql_append_int8(sabot_sql_appender appender, int8_t value) {
	return sabot_sql_append_internal<int8_t>(appender, value);
}

sabot_sql_state sabot_sql_append_int16(sabot_sql_appender appender, int16_t value) {
	return sabot_sql_append_internal<int16_t>(appender, value);
}

sabot_sql_state sabot_sql_append_int32(sabot_sql_appender appender, int32_t value) {
	return sabot_sql_append_internal<int32_t>(appender, value);
}

sabot_sql_state sabot_sql_append_int64(sabot_sql_appender appender, int64_t value) {
	return sabot_sql_append_internal<int64_t>(appender, value);
}

sabot_sql_state sabot_sql_append_hugeint(sabot_sql_appender appender, sabot_sql_hugeint value) {
	hugeint_t internal;
	internal.lower = value.lower;
	internal.upper = value.upper;
	return sabot_sql_append_internal<hugeint_t>(appender, internal);
}

sabot_sql_state sabot_sql_append_uint8(sabot_sql_appender appender, uint8_t value) {
	return sabot_sql_append_internal<uint8_t>(appender, value);
}

sabot_sql_state sabot_sql_append_uint16(sabot_sql_appender appender, uint16_t value) {
	return sabot_sql_append_internal<uint16_t>(appender, value);
}

sabot_sql_state sabot_sql_append_uint32(sabot_sql_appender appender, uint32_t value) {
	return sabot_sql_append_internal<uint32_t>(appender, value);
}

sabot_sql_state sabot_sql_append_uint64(sabot_sql_appender appender, uint64_t value) {
	return sabot_sql_append_internal<uint64_t>(appender, value);
}

sabot_sql_state sabot_sql_append_uhugeint(sabot_sql_appender appender, sabot_sql_uhugeint value) {
	uhugeint_t internal;
	internal.lower = value.lower;
	internal.upper = value.upper;
	return sabot_sql_append_internal<uhugeint_t>(appender, internal);
}

sabot_sql_state sabot_sql_append_float(sabot_sql_appender appender, float value) {
	return sabot_sql_append_internal<float>(appender, value);
}

sabot_sql_state sabot_sql_append_double(sabot_sql_appender appender, double value) {
	return sabot_sql_append_internal<double>(appender, value);
}

sabot_sql_state sabot_sql_append_date(sabot_sql_appender appender, sabot_sql_date value) {
	return sabot_sql_append_internal<date_t>(appender, date_t(value.days));
}

sabot_sql_state sabot_sql_append_time(sabot_sql_appender appender, sabot_sql_time value) {
	return sabot_sql_append_internal<dtime_t>(appender, dtime_t(value.micros));
}

sabot_sql_state sabot_sql_append_timestamp(sabot_sql_appender appender, sabot_sql_timestamp value) {
	return sabot_sql_append_internal<timestamp_t>(appender, timestamp_t(value.micros));
}

sabot_sql_state sabot_sql_append_interval(sabot_sql_appender appender, sabot_sql_interval value) {
	interval_t interval;
	interval.months = value.months;
	interval.days = value.days;
	interval.micros = value.micros;
	return sabot_sql_append_internal<interval_t>(appender, interval);
}

sabot_sql_state sabot_sql_append_null(sabot_sql_appender appender) {
	return sabot_sql_append_internal<std::nullptr_t>(appender, nullptr);
}

sabot_sql_state sabot_sql_append_varchar(sabot_sql_appender appender, const char *val) {
	return sabot_sql_append_internal<const char *>(appender, val);
}

sabot_sql_state sabot_sql_append_varchar_length(sabot_sql_appender appender, const char *val, idx_t length) {
	return sabot_sql_append_internal<string_t>(appender, string_t(val, sabot_sql::UnsafeNumericCast<uint32_t>(length)));
}

sabot_sql_state sabot_sql_append_blob(sabot_sql_appender appender, const void *data, idx_t length) {
	auto value = sabot_sql::Value::BLOB(sabot_sql::const_data_ptr_cast(data), length);
	return sabot_sql_append_internal<sabot_sql::Value>(appender, value);
}

sabot_sql_state sabot_sql_appender_flush(sabot_sql_appender appender_p) {
	return sabot_sql_appender_run_function(appender_p, [&](BaseAppender &appender) { appender.Flush(); });
}

sabot_sql_state sabot_sql_appender_close(sabot_sql_appender appender_p) {
	return sabot_sql_appender_run_function(appender_p, [&](BaseAppender &appender) { appender.Close(); });
}

idx_t sabot_sql_appender_column_count(sabot_sql_appender appender) {
	if (!appender) {
		return 0;
	}

	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		return 0;
	}

	return wrapper->appender->GetActiveTypes().size();
}

sabot_sql_logical_type sabot_sql_appender_column_type(sabot_sql_appender appender, idx_t col_idx) {
	if (!appender || col_idx >= sabot_sql_appender_column_count(appender)) {
		return nullptr;
	}

	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		return nullptr;
	}

	auto &logical_type = wrapper->appender->GetActiveTypes()[col_idx];
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(logical_type));
}

sabot_sql_state sabot_sql_append_value(sabot_sql_appender appender, sabot_sql_value value) {
	return sabot_sql_append_internal<sabot_sql::Value>(appender, *(reinterpret_cast<sabot_sql::Value *>(value)));
}

sabot_sql_state sabot_sql_append_data_chunk(sabot_sql_appender appender_p, sabot_sql_data_chunk chunk) {
	if (!chunk) {
		return SabotSQLError;
	}
	auto data_chunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);
	return sabot_sql_appender_run_function(appender_p,
	                                    [&](BaseAppender &appender) { appender.AppendDataChunk(*data_chunk); });
}
