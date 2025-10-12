#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/main/prepared_statement_data.hpp"
#include "sabot_sql/common/types/decimal.hpp"
#include "sabot_sql/common/uhugeint.hpp"
#include "sabot_sql/common/optional_ptr.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"

using sabot_sql::case_insensitive_map_t;
using sabot_sql::Connection;
using sabot_sql::date_t;
using sabot_sql::dtime_t;
using sabot_sql::ErrorData;
using sabot_sql::ExtractStatementsWrapper;
using sabot_sql::hugeint_t;
using sabot_sql::LogicalType;
using sabot_sql::MaterializedQueryResult;
using sabot_sql::optional_ptr;
using sabot_sql::PreparedStatementWrapper;
using sabot_sql::QueryResultType;
using sabot_sql::StringUtil;
using sabot_sql::timestamp_t;
using sabot_sql::uhugeint_t;
using sabot_sql::Value;

idx_t sabot_sql_extract_statements(sabot_sql_connection connection, const char *query,
                                sabot_sql_extracted_statements *out_extracted_statements) {
	if (!connection || !query || !out_extracted_statements) {
		return 0;
	}
	auto wrapper = new ExtractStatementsWrapper();
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		wrapper->statements = conn->ExtractStatements(query);
	} catch (const std::exception &ex) {
		ErrorData error(ex);
		wrapper->error = error.Message();
	}

	*out_extracted_statements = (sabot_sql_extracted_statements)wrapper;
	return wrapper->statements.size();
}

sabot_sql_state sabot_sql_prepare_extracted_statement(sabot_sql_connection connection,
                                                sabot_sql_extracted_statements extracted_statements, idx_t index,
                                                sabot_sql_prepared_statement *out_prepared_statement) {
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto source_wrapper = (ExtractStatementsWrapper *)extracted_statements;

	if (!connection || !out_prepared_statement || index >= source_wrapper->statements.size()) {
		return SabotSQLError;
	}
	auto wrapper = new PreparedStatementWrapper();
	try {
		wrapper->statement = conn->Prepare(std::move(source_wrapper->statements[index]));
		*out_prepared_statement = (sabot_sql_prepared_statement)wrapper;
		return wrapper->statement->HasError() ? SabotSQLError : SabotSQLSuccess;
	} catch (...) {
		delete wrapper;
		return SabotSQLError;
	}
}

const char *sabot_sql_extract_statements_error(sabot_sql_extracted_statements extracted_statements) {
	auto wrapper = (ExtractStatementsWrapper *)extracted_statements;
	if (!wrapper || wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

sabot_sql_state sabot_sql_prepare(sabot_sql_connection connection, const char *query,
                            sabot_sql_prepared_statement *out_prepared_statement) {
	if (!connection || !query || !out_prepared_statement) {
		return SabotSQLError;
	}
	auto wrapper = new PreparedStatementWrapper();
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		wrapper->statement = conn->Prepare(query);
		*out_prepared_statement = reinterpret_cast<sabot_sql_prepared_statement>(wrapper);
		return !wrapper->statement->HasError() ? SabotSQLSuccess : SabotSQLError;
	} catch (...) {
		delete wrapper;
		return SabotSQLError;
	}
}

const char *sabot_sql_prepare_error(sabot_sql_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || !wrapper->statement->HasError()) {
		return nullptr;
	}
	return wrapper->statement->error.Message().c_str();
}

idx_t sabot_sql_nparams(sabot_sql_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return 0;
	}
	return wrapper->statement->named_param_map.size();
}

static sabot_sql::string sabot_sql_parameter_name_internal(sabot_sql_prepared_statement prepared_statement, idx_t index) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return sabot_sql::string();
	}
	if (index > wrapper->statement->named_param_map.size()) {
		return sabot_sql::string();
	}
	for (auto &item : wrapper->statement->named_param_map) {
		auto &identifier = item.first;
		auto &param_idx = item.second;
		if (param_idx == index) {
			// Found the matching parameter
			return identifier;
		}
	}
	// No parameter was found with this index
	return sabot_sql::string();
}

const char *sabot_sql_parameter_name(sabot_sql_prepared_statement prepared_statement, idx_t index) {
	auto identifier = sabot_sql_parameter_name_internal(prepared_statement, index);
	if (identifier == sabot_sql::string()) {
		return NULL;
	}
	return strdup(identifier.c_str());
}

sabot_sql_type sabot_sql_param_type(sabot_sql_prepared_statement prepared_statement, idx_t param_idx) {
	auto logical_type = sabot_sql_param_logical_type(prepared_statement, param_idx);
	if (!logical_type) {
		return SABOT_SQL_TYPE_INVALID;
	}

	auto type = sabot_sql_get_type_id(logical_type);
	sabot_sql_destroy_logical_type(&logical_type);

	return type;
}

sabot_sql_logical_type sabot_sql_param_logical_type(sabot_sql_prepared_statement prepared_statement, idx_t param_idx) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return nullptr;
	}

	auto identifier = sabot_sql_parameter_name_internal(prepared_statement, param_idx);
	if (identifier == sabot_sql::string()) {
		return nullptr;
	}

	LogicalType param_type;

	if (wrapper->statement->data->TryGetType(identifier, param_type)) {
		return reinterpret_cast<sabot_sql_logical_type>(new LogicalType(param_type));
	}
	// The value_map is gone after executing the prepared statement
	// See if this is the case and we still have a value registered for it
	auto it = wrapper->values.find(identifier);
	if (it != wrapper->values.end()) {
		return reinterpret_cast<sabot_sql_logical_type>(new LogicalType(it->second.return_type));
	}
	return nullptr;
}

sabot_sql_state sabot_sql_clear_bindings(sabot_sql_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return SabotSQLError;
	}
	wrapper->values.clear();
	return SabotSQLSuccess;
}

idx_t sabot_sql_prepared_statement_column_count(sabot_sql_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return 0;
	}
	return wrapper->statement->ColumnCount();
}

const char *sabot_sql_prepared_statement_column_name(sabot_sql_prepared_statement prepared_statement, idx_t col_idx) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return nullptr;
	}
	auto &names = wrapper->statement->GetNames();

	if (col_idx < 0 || col_idx >= names.size()) {
		return nullptr;
	}
	return strdup(names[col_idx].c_str());
}

sabot_sql_logical_type sabot_sql_prepared_statement_column_logical_type(sabot_sql_prepared_statement prepared_statement,
                                                                  idx_t col_idx) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return nullptr;
	}
	auto types = wrapper->statement->GetTypes();
	if (col_idx < 0 || col_idx >= types.size()) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_logical_type>(new LogicalType(types[col_idx]));
}

sabot_sql_type sabot_sql_prepared_statement_column_type(sabot_sql_prepared_statement prepared_statement, idx_t col_idx) {
	auto logical_type = sabot_sql_prepared_statement_column_logical_type(prepared_statement, col_idx);
	if (!logical_type) {
		return SABOT_SQL_TYPE_INVALID;
	}

	auto type = sabot_sql_get_type_id(logical_type);
	sabot_sql_destroy_logical_type(&logical_type);

	return type;
}

sabot_sql_state sabot_sql_bind_value(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_value val) {
	auto value = reinterpret_cast<Value *>(val);
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return SabotSQLError;
	}
	if (param_idx <= 0 || param_idx > wrapper->statement->named_param_map.size()) {
		wrapper->statement->error =
		    sabot_sql::InvalidInputException("Can not bind to parameter number %d, statement only has %d parameter(s)",
		                                  param_idx, wrapper->statement->named_param_map.size());
		return SabotSQLError;
	}
	auto identifier = sabot_sql_parameter_name_internal(prepared_statement, param_idx);
	wrapper->values[identifier] = sabot_sql::BoundParameterData(*value);
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_bind_parameter_index(sabot_sql_prepared_statement prepared_statement, idx_t *param_idx_out,
                                         const char *name_p) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return SabotSQLError;
	}
	if (!name_p || !param_idx_out) {
		return SabotSQLError;
	}
	auto name = std::string(name_p);
	for (auto &pair : wrapper->statement->named_param_map) {
		if (sabot_sql::StringUtil::CIEquals(pair.first, name)) {
			*param_idx_out = pair.second;
			return SabotSQLSuccess;
		}
	}
	return SabotSQLError;
}

sabot_sql_state sabot_sql_bind_boolean(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, bool val) {
	auto value = Value::BOOLEAN(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_int8(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int8_t val) {
	auto value = Value::TINYINT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_int16(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int16_t val) {
	auto value = Value::SMALLINT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_int32(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int32_t val) {
	auto value = Value::INTEGER(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_int64(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int64_t val) {
	auto value = Value::BIGINT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

static hugeint_t sabot_sql_internal_hugeint(sabot_sql_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

static uhugeint_t sabot_sql_internal_uhugeint(sabot_sql_uhugeint val) {
	uhugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

sabot_sql_state sabot_sql_bind_hugeint(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_hugeint val) {
	auto value = Value::HUGEINT(sabot_sql_internal_hugeint(val));
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_uhugeint(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_uhugeint val) {
	auto value = Value::UHUGEINT(sabot_sql_internal_uhugeint(val));
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_uint8(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint8_t val) {
	auto value = Value::UTINYINT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_uint16(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint16_t val) {
	auto value = Value::USMALLINT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_uint32(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint32_t val) {
	auto value = Value::UINTEGER(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_uint64(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint64_t val) {
	auto value = Value::UBIGINT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_float(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, float val) {
	auto value = Value::FLOAT(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_double(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, double val) {
	auto value = Value::DOUBLE(val);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_date(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_date val) {
	auto value = Value::DATE(date_t(val.days));
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_time(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_time val) {
	auto value = Value::TIME(dtime_t(val.micros));
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_timestamp(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
                                   sabot_sql_timestamp val) {
	auto value = Value::TIMESTAMP(timestamp_t(val.micros));
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_timestamp_tz(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
                                      sabot_sql_timestamp val) {
	auto value = Value::TIMESTAMPTZ(sabot_sql::timestamp_tz_t(val.micros));
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_interval(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_interval val) {
	auto value = Value::INTERVAL(val.months, val.days, val.micros);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_varchar(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, const char *val) {
	try {
		auto value = Value(val);
		return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
	} catch (...) {
		return SabotSQLError;
	}
}

sabot_sql_state sabot_sql_bind_varchar_length(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, const char *val,
                                        idx_t length) {
	try {
		auto value = Value(std::string(val, length));
		return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
	} catch (...) {
		return SabotSQLError;
	}
}

sabot_sql_state sabot_sql_bind_decimal(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_decimal val) {
	auto hugeint_val = sabot_sql_internal_hugeint(val.value);
	if (val.width > sabot_sql::Decimal::MAX_WIDTH_INT64) {
		auto value = Value::DECIMAL(hugeint_val, val.width, val.scale);
		return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
	}
	auto value = hugeint_val.lower;
	auto duck_val = Value::DECIMAL((int64_t)value, val.width, val.scale);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&duck_val);
}

sabot_sql_state sabot_sql_bind_blob(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, const void *data,
                              idx_t length) {
	auto value = Value::BLOB(sabot_sql::const_data_ptr_cast(data), length);
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_bind_null(sabot_sql_prepared_statement prepared_statement, idx_t param_idx) {
	auto value = Value();
	return sabot_sql_bind_value(prepared_statement, param_idx, (sabot_sql_value)&value);
}

sabot_sql_state sabot_sql_execute_prepared(sabot_sql_prepared_statement prepared_statement, sabot_sql_result *out_result) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return SabotSQLError;
	}

	sabot_sql::unique_ptr<sabot_sql::QueryResult> result;
	try {
		result = wrapper->statement->Execute(wrapper->values, false);
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLTranslateResult(std::move(result), out_result);
}

sabot_sql_state sabot_sql_execute_prepared_streaming(sabot_sql_prepared_statement prepared_statement,
                                               sabot_sql_result *out_result) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return SabotSQLError;
	}

	try {
		auto result = wrapper->statement->Execute(wrapper->values, true);
		return SabotSQLTranslateResult(std::move(result), out_result);
	} catch (...) {
		return SabotSQLError;
	}
}

sabot_sql_statement_type sabot_sql_prepared_statement_type(sabot_sql_prepared_statement statement) {
	if (!statement) {
		return SABOT_SQL_STATEMENT_TYPE_INVALID;
	}
	auto stmt = reinterpret_cast<PreparedStatementWrapper *>(statement);

	return StatementTypeToC(stmt->statement->GetStatementType());
}

template <class T>
void sabot_sql_destroy(void **wrapper) {
	if (!wrapper) {
		return;
	}

	auto casted = (T *)*wrapper;
	if (casted) {
		delete casted;
	}
	*wrapper = nullptr;
}

void sabot_sql_destroy_extracted(sabot_sql_extracted_statements *extracted_statements) {
	sabot_sql_destroy<ExtractStatementsWrapper>(reinterpret_cast<void **>(extracted_statements));
}

void sabot_sql_destroy_prepare(sabot_sql_prepared_statement *prepared_statement) {
	sabot_sql_destroy<PreparedStatementWrapper>(reinterpret_cast<void **>(prepared_statement));
}
