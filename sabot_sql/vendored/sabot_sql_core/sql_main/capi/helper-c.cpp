#include "sabot_sql/main/capi/capi_internal.hpp"

namespace sabot_sql {

LogicalTypeId LogicalTypeIdFromC(const sabot_sql_type type) {
	switch (type) {
	case SABOT_SQL_TYPE_INVALID:
		return LogicalTypeId::INVALID;
	case SABOT_SQL_TYPE_BOOLEAN:
		return LogicalTypeId::BOOLEAN;
	case SABOT_SQL_TYPE_TINYINT:
		return LogicalTypeId::TINYINT;
	case SABOT_SQL_TYPE_SMALLINT:
		return LogicalTypeId::SMALLINT;
	case SABOT_SQL_TYPE_INTEGER:
		return LogicalTypeId::INTEGER;
	case SABOT_SQL_TYPE_BIGINT:
		return LogicalTypeId::BIGINT;
	case SABOT_SQL_TYPE_UTINYINT:
		return LogicalTypeId::UTINYINT;
	case SABOT_SQL_TYPE_USMALLINT:
		return LogicalTypeId::USMALLINT;
	case SABOT_SQL_TYPE_UINTEGER:
		return LogicalTypeId::UINTEGER;
	case SABOT_SQL_TYPE_UBIGINT:
		return LogicalTypeId::UBIGINT;
	case SABOT_SQL_TYPE_FLOAT:
		return LogicalTypeId::FLOAT;
	case SABOT_SQL_TYPE_DOUBLE:
		return LogicalTypeId::DOUBLE;
	case SABOT_SQL_TYPE_TIMESTAMP:
		return LogicalTypeId::TIMESTAMP;
	case SABOT_SQL_TYPE_DATE:
		return LogicalTypeId::DATE;
	case SABOT_SQL_TYPE_TIME:
		return LogicalTypeId::TIME;
	case SABOT_SQL_TYPE_INTERVAL:
		return LogicalTypeId::INTERVAL;
	case SABOT_SQL_TYPE_HUGEINT:
		return LogicalTypeId::HUGEINT;
	case SABOT_SQL_TYPE_UHUGEINT:
		return LogicalTypeId::UHUGEINT;
	case SABOT_SQL_TYPE_VARCHAR:
		return LogicalTypeId::VARCHAR;
	case SABOT_SQL_TYPE_BLOB:
		return LogicalTypeId::BLOB;
	case SABOT_SQL_TYPE_DECIMAL:
		return LogicalTypeId::DECIMAL;
	case SABOT_SQL_TYPE_TIMESTAMP_S:
		return LogicalTypeId::TIMESTAMP_SEC;
	case SABOT_SQL_TYPE_TIMESTAMP_MS:
		return LogicalTypeId::TIMESTAMP_MS;
	case SABOT_SQL_TYPE_TIMESTAMP_NS:
		return LogicalTypeId::TIMESTAMP_NS;
	case SABOT_SQL_TYPE_ENUM:
		return LogicalTypeId::ENUM;
	case SABOT_SQL_TYPE_LIST:
		return LogicalTypeId::LIST;
	case SABOT_SQL_TYPE_STRUCT:
		return LogicalTypeId::STRUCT;
	case SABOT_SQL_TYPE_MAP:
		return LogicalTypeId::MAP;
	case SABOT_SQL_TYPE_ARRAY:
		return LogicalTypeId::ARRAY;
	case SABOT_SQL_TYPE_UUID:
		return LogicalTypeId::UUID;
	case SABOT_SQL_TYPE_UNION:
		return LogicalTypeId::UNION;
	case SABOT_SQL_TYPE_BIT:
		return LogicalTypeId::BIT;
	case SABOT_SQL_TYPE_TIME_TZ:
		return LogicalTypeId::TIME_TZ;
	case SABOT_SQL_TYPE_TIMESTAMP_TZ:
		return LogicalTypeId::TIMESTAMP_TZ;
	case SABOT_SQL_TYPE_ANY:
		return LogicalTypeId::ANY;
	case SABOT_SQL_TYPE_BIGNUM:
		return LogicalTypeId::BIGNUM;
	case SABOT_SQL_TYPE_SQLNULL:
		return LogicalTypeId::SQLNULL;
	case SABOT_SQL_TYPE_STRING_LITERAL:
		return LogicalTypeId::STRING_LITERAL;
	case SABOT_SQL_TYPE_INTEGER_LITERAL:
		return LogicalTypeId::INTEGER_LITERAL;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return LogicalTypeId::INVALID;
	} // LCOV_EXCL_STOP
}

sabot_sql_type LogicalTypeIdToC(const LogicalTypeId type) {
	switch (type) {
	case LogicalTypeId::INVALID:
		return SABOT_SQL_TYPE_INVALID;
	case LogicalTypeId::UNKNOWN:
		return SABOT_SQL_TYPE_INVALID;
	case LogicalTypeId::BOOLEAN:
		return SABOT_SQL_TYPE_BOOLEAN;
	case LogicalTypeId::TINYINT:
		return SABOT_SQL_TYPE_TINYINT;
	case LogicalTypeId::SMALLINT:
		return SABOT_SQL_TYPE_SMALLINT;
	case LogicalTypeId::INTEGER:
		return SABOT_SQL_TYPE_INTEGER;
	case LogicalTypeId::BIGINT:
		return SABOT_SQL_TYPE_BIGINT;
	case LogicalTypeId::UTINYINT:
		return SABOT_SQL_TYPE_UTINYINT;
	case LogicalTypeId::USMALLINT:
		return SABOT_SQL_TYPE_USMALLINT;
	case LogicalTypeId::UINTEGER:
		return SABOT_SQL_TYPE_UINTEGER;
	case LogicalTypeId::UBIGINT:
		return SABOT_SQL_TYPE_UBIGINT;
	case LogicalTypeId::HUGEINT:
		return SABOT_SQL_TYPE_HUGEINT;
	case LogicalTypeId::UHUGEINT:
		return SABOT_SQL_TYPE_UHUGEINT;
	case LogicalTypeId::FLOAT:
		return SABOT_SQL_TYPE_FLOAT;
	case LogicalTypeId::DOUBLE:
		return SABOT_SQL_TYPE_DOUBLE;
	case LogicalTypeId::TIMESTAMP:
		return SABOT_SQL_TYPE_TIMESTAMP;
	case LogicalTypeId::TIMESTAMP_TZ:
		return SABOT_SQL_TYPE_TIMESTAMP_TZ;
	case LogicalTypeId::TIMESTAMP_SEC:
		return SABOT_SQL_TYPE_TIMESTAMP_S;
	case LogicalTypeId::TIMESTAMP_MS:
		return SABOT_SQL_TYPE_TIMESTAMP_MS;
	case LogicalTypeId::TIMESTAMP_NS:
		return SABOT_SQL_TYPE_TIMESTAMP_NS;
	case LogicalTypeId::DATE:
		return SABOT_SQL_TYPE_DATE;
	case LogicalTypeId::TIME:
		return SABOT_SQL_TYPE_TIME;
	case LogicalTypeId::TIME_TZ:
		return SABOT_SQL_TYPE_TIME_TZ;
	case LogicalTypeId::VARCHAR:
		return SABOT_SQL_TYPE_VARCHAR;
	case LogicalTypeId::BLOB:
		return SABOT_SQL_TYPE_BLOB;
	case LogicalTypeId::BIT:
		return SABOT_SQL_TYPE_BIT;
	case LogicalTypeId::BIGNUM:
		return SABOT_SQL_TYPE_BIGNUM;
	case LogicalTypeId::INTERVAL:
		return SABOT_SQL_TYPE_INTERVAL;
	case LogicalTypeId::DECIMAL:
		return SABOT_SQL_TYPE_DECIMAL;
	case LogicalTypeId::ENUM:
		return SABOT_SQL_TYPE_ENUM;
	case LogicalTypeId::LIST:
		return SABOT_SQL_TYPE_LIST;
	case LogicalTypeId::STRUCT:
		return SABOT_SQL_TYPE_STRUCT;
	case LogicalTypeId::MAP:
		return SABOT_SQL_TYPE_MAP;
	case LogicalTypeId::UNION:
		return SABOT_SQL_TYPE_UNION;
	case LogicalTypeId::UUID:
		return SABOT_SQL_TYPE_UUID;
	case LogicalTypeId::ARRAY:
		return SABOT_SQL_TYPE_ARRAY;
	case LogicalTypeId::ANY:
		return SABOT_SQL_TYPE_ANY;
	case LogicalTypeId::SQLNULL:
		return SABOT_SQL_TYPE_SQLNULL;
	case LogicalTypeId::STRING_LITERAL:
		return SABOT_SQL_TYPE_STRING_LITERAL;
	case LogicalTypeId::INTEGER_LITERAL:
		return SABOT_SQL_TYPE_INTEGER_LITERAL;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return SABOT_SQL_TYPE_INVALID;
	} // LCOV_EXCL_STOP
}

idx_t GetCTypeSize(const sabot_sql_type type) {
	switch (type) {
	case SABOT_SQL_TYPE_BOOLEAN:
		return sizeof(bool);
	case SABOT_SQL_TYPE_TINYINT:
		return sizeof(int8_t);
	case SABOT_SQL_TYPE_SMALLINT:
		return sizeof(int16_t);
	case SABOT_SQL_TYPE_INTEGER:
		return sizeof(int32_t);
	case SABOT_SQL_TYPE_BIGINT:
		return sizeof(int64_t);
	case SABOT_SQL_TYPE_UTINYINT:
		return sizeof(uint8_t);
	case SABOT_SQL_TYPE_USMALLINT:
		return sizeof(uint16_t);
	case SABOT_SQL_TYPE_UINTEGER:
		return sizeof(uint32_t);
	case SABOT_SQL_TYPE_UBIGINT:
		return sizeof(uint64_t);
	case SABOT_SQL_TYPE_UHUGEINT:
	case SABOT_SQL_TYPE_HUGEINT:
	case SABOT_SQL_TYPE_UUID:
		return sizeof(sabot_sql_hugeint);
	case SABOT_SQL_TYPE_FLOAT:
		return sizeof(float);
	case SABOT_SQL_TYPE_DOUBLE:
		return sizeof(double);
	case SABOT_SQL_TYPE_DATE:
		return sizeof(sabot_sql_date);
	case SABOT_SQL_TYPE_TIME:
		return sizeof(sabot_sql_time);
	case SABOT_SQL_TYPE_TIMESTAMP:
	case SABOT_SQL_TYPE_TIMESTAMP_TZ:
	case SABOT_SQL_TYPE_TIMESTAMP_S:
	case SABOT_SQL_TYPE_TIMESTAMP_MS:
	case SABOT_SQL_TYPE_TIMESTAMP_NS:
		return sizeof(sabot_sql_timestamp);
	case SABOT_SQL_TYPE_VARCHAR:
		return sizeof(const char *);
	case SABOT_SQL_TYPE_BLOB:
		return sizeof(sabot_sql_blob);
	case SABOT_SQL_TYPE_INTERVAL:
		return sizeof(sabot_sql_interval);
	case SABOT_SQL_TYPE_DECIMAL:
		return sizeof(sabot_sql_hugeint);
	default: // LCOV_EXCL_START
		// Unsupported nested or complex type. Internally, we set the null mask to NULL.
		// This is a deprecated code path. Use the Vector Interface for nested and complex types.
		return 0;
	} // LCOV_EXCL_STOP
}

sabot_sql_statement_type StatementTypeToC(const StatementType type) {
	switch (type) {
	case StatementType::SELECT_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_SELECT;
	case StatementType::INVALID_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_INVALID;
	case StatementType::INSERT_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_INSERT;
	case StatementType::UPDATE_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_UPDATE;
	case StatementType::EXPLAIN_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_EXPLAIN;
	case StatementType::DELETE_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_DELETE;
	case StatementType::PREPARE_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_PREPARE;
	case StatementType::CREATE_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_CREATE;
	case StatementType::EXECUTE_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_EXECUTE;
	case StatementType::ALTER_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_ALTER;
	case StatementType::TRANSACTION_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_TRANSACTION;
	case StatementType::COPY_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_COPY;
	case StatementType::ANALYZE_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_ANALYZE;
	case StatementType::VARIABLE_SET_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_VARIABLE_SET;
	case StatementType::CREATE_FUNC_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_CREATE_FUNC;
	case StatementType::DROP_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_DROP;
	case StatementType::EXPORT_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_EXPORT;
	case StatementType::PRAGMA_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_PRAGMA;
	case StatementType::VACUUM_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_VACUUM;
	case StatementType::CALL_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_CALL;
	case StatementType::SET_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_SET;
	case StatementType::LOAD_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_LOAD;
	case StatementType::RELATION_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_RELATION;
	case StatementType::EXTENSION_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_EXTENSION;
	case StatementType::LOGICAL_PLAN_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_LOGICAL_PLAN;
	case StatementType::ATTACH_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_ATTACH;
	case StatementType::DETACH_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_DETACH;
	case StatementType::MULTI_STATEMENT:
		return SABOT_SQL_STATEMENT_TYPE_MULTI;
	default:
		return SABOT_SQL_STATEMENT_TYPE_INVALID;
	}
}

sabot_sql_error_type ErrorTypeToC(const ExceptionType type) {
	switch (type) {
	case ExceptionType::INVALID:
		return SABOT_SQL_ERROR_INVALID;
	case ExceptionType::OUT_OF_RANGE:
		return SABOT_SQL_ERROR_OUT_OF_RANGE;
	case ExceptionType::CONVERSION:
		return SABOT_SQL_ERROR_CONVERSION;
	case ExceptionType::UNKNOWN_TYPE:
		return SABOT_SQL_ERROR_UNKNOWN_TYPE;
	case ExceptionType::DECIMAL:
		return SABOT_SQL_ERROR_DECIMAL;
	case ExceptionType::MISMATCH_TYPE:
		return SABOT_SQL_ERROR_MISMATCH_TYPE;
	case ExceptionType::DIVIDE_BY_ZERO:
		return SABOT_SQL_ERROR_DIVIDE_BY_ZERO;
	case ExceptionType::OBJECT_SIZE:
		return SABOT_SQL_ERROR_OBJECT_SIZE;
	case ExceptionType::INVALID_TYPE:
		return SABOT_SQL_ERROR_INVALID_TYPE;
	case ExceptionType::SERIALIZATION:
		return SABOT_SQL_ERROR_SERIALIZATION;
	case ExceptionType::TRANSACTION:
		return SABOT_SQL_ERROR_TRANSACTION;
	case ExceptionType::NOT_IMPLEMENTED:
		return SABOT_SQL_ERROR_NOT_IMPLEMENTED;
	case ExceptionType::EXPRESSION:
		return SABOT_SQL_ERROR_EXPRESSION;
	case ExceptionType::CATALOG:
		return SABOT_SQL_ERROR_CATALOG;
	case ExceptionType::PARSER:
		return SABOT_SQL_ERROR_PARSER;
	case ExceptionType::PLANNER:
		return SABOT_SQL_ERROR_PLANNER;
	case ExceptionType::SCHEDULER:
		return SABOT_SQL_ERROR_SCHEDULER;
	case ExceptionType::EXECUTOR:
		return SABOT_SQL_ERROR_EXECUTOR;
	case ExceptionType::CONSTRAINT:
		return SABOT_SQL_ERROR_CONSTRAINT;
	case ExceptionType::INDEX:
		return SABOT_SQL_ERROR_INDEX;
	case ExceptionType::STAT:
		return SABOT_SQL_ERROR_STAT;
	case ExceptionType::CONNECTION:
		return SABOT_SQL_ERROR_CONNECTION;
	case ExceptionType::SYNTAX:
		return SABOT_SQL_ERROR_SYNTAX;
	case ExceptionType::SETTINGS:
		return SABOT_SQL_ERROR_SETTINGS;
	case ExceptionType::BINDER:
		return SABOT_SQL_ERROR_BINDER;
	case ExceptionType::NETWORK:
		return SABOT_SQL_ERROR_NETWORK;
	case ExceptionType::OPTIMIZER:
		return SABOT_SQL_ERROR_OPTIMIZER;
	case ExceptionType::NULL_POINTER:
		return SABOT_SQL_ERROR_NULL_POINTER;
	case ExceptionType::IO:
		return SABOT_SQL_ERROR_IO;
	case ExceptionType::INTERRUPT:
		return SABOT_SQL_ERROR_INTERRUPT;
	case ExceptionType::FATAL:
		return SABOT_SQL_ERROR_FATAL;
	case ExceptionType::INTERNAL:
		return SABOT_SQL_ERROR_INTERNAL;
	case ExceptionType::INVALID_INPUT:
		return SABOT_SQL_ERROR_INVALID_INPUT;
	case ExceptionType::OUT_OF_MEMORY:
		return SABOT_SQL_ERROR_OUT_OF_MEMORY;
	case ExceptionType::PERMISSION:
		return SABOT_SQL_ERROR_PERMISSION;
	case ExceptionType::PARAMETER_NOT_RESOLVED:
		return SABOT_SQL_ERROR_PARAMETER_NOT_RESOLVED;
	case ExceptionType::PARAMETER_NOT_ALLOWED:
		return SABOT_SQL_ERROR_PARAMETER_NOT_ALLOWED;
	case ExceptionType::DEPENDENCY:
		return SABOT_SQL_ERROR_DEPENDENCY;
	case ExceptionType::HTTP:
		return SABOT_SQL_ERROR_HTTP;
	case ExceptionType::MISSING_EXTENSION:
		return SABOT_SQL_ERROR_MISSING_EXTENSION;
	case ExceptionType::AUTOLOAD:
		return SABOT_SQL_ERROR_AUTOLOAD;
	case ExceptionType::SEQUENCE:
		return SABOT_SQL_ERROR_SEQUENCE;
	case ExceptionType::INVALID_CONFIGURATION:
		return SABOT_SQL_INVALID_CONFIGURATION;
	default:
		return SABOT_SQL_ERROR_INVALID;
	}
}

ExceptionType ErrorTypeFromC(const sabot_sql_error_type type) {
	switch (type) {
	case SABOT_SQL_ERROR_INVALID:
		return ExceptionType::INVALID;
	case SABOT_SQL_ERROR_OUT_OF_RANGE:
		return ExceptionType::OUT_OF_RANGE;
	case SABOT_SQL_ERROR_CONVERSION:
		return ExceptionType::CONVERSION;
	case SABOT_SQL_ERROR_UNKNOWN_TYPE:
		return ExceptionType::UNKNOWN_TYPE;
	case SABOT_SQL_ERROR_DECIMAL:
		return ExceptionType::DECIMAL;
	case SABOT_SQL_ERROR_MISMATCH_TYPE:
		return ExceptionType::MISMATCH_TYPE;
	case SABOT_SQL_ERROR_DIVIDE_BY_ZERO:
		return ExceptionType::DIVIDE_BY_ZERO;
	case SABOT_SQL_ERROR_OBJECT_SIZE:
		return ExceptionType::OBJECT_SIZE;
	case SABOT_SQL_ERROR_INVALID_TYPE:
		return ExceptionType::INVALID_TYPE;
	case SABOT_SQL_ERROR_SERIALIZATION:
		return ExceptionType::SERIALIZATION;
	case SABOT_SQL_ERROR_TRANSACTION:
		return ExceptionType::TRANSACTION;
	case SABOT_SQL_ERROR_NOT_IMPLEMENTED:
		return ExceptionType::NOT_IMPLEMENTED;
	case SABOT_SQL_ERROR_EXPRESSION:
		return ExceptionType::EXPRESSION;
	case SABOT_SQL_ERROR_CATALOG:
		return ExceptionType::CATALOG;
	case SABOT_SQL_ERROR_PARSER:
		return ExceptionType::PARSER;
	case SABOT_SQL_ERROR_PLANNER:
		return ExceptionType::PLANNER;
	case SABOT_SQL_ERROR_SCHEDULER:
		return ExceptionType::SCHEDULER;
	case SABOT_SQL_ERROR_EXECUTOR:
		return ExceptionType::EXECUTOR;
	case SABOT_SQL_ERROR_CONSTRAINT:
		return ExceptionType::CONSTRAINT;
	case SABOT_SQL_ERROR_INDEX:
		return ExceptionType::INDEX;
	case SABOT_SQL_ERROR_STAT:
		return ExceptionType::STAT;
	case SABOT_SQL_ERROR_CONNECTION:
		return ExceptionType::CONNECTION;
	case SABOT_SQL_ERROR_SYNTAX:
		return ExceptionType::SYNTAX;
	case SABOT_SQL_ERROR_SETTINGS:
		return ExceptionType::SETTINGS;
	case SABOT_SQL_ERROR_BINDER:
		return ExceptionType::BINDER;
	case SABOT_SQL_ERROR_NETWORK:
		return ExceptionType::NETWORK;
	case SABOT_SQL_ERROR_OPTIMIZER:
		return ExceptionType::OPTIMIZER;
	case SABOT_SQL_ERROR_NULL_POINTER:
		return ExceptionType::NULL_POINTER;
	case SABOT_SQL_ERROR_IO:
		return ExceptionType::IO;
	case SABOT_SQL_ERROR_INTERRUPT:
		return ExceptionType::INTERRUPT;
	case SABOT_SQL_ERROR_FATAL:
		return ExceptionType::FATAL;
	case SABOT_SQL_ERROR_INTERNAL:
		return ExceptionType::INTERNAL;
	case SABOT_SQL_ERROR_INVALID_INPUT:
		return ExceptionType::INVALID_INPUT;
	case SABOT_SQL_ERROR_OUT_OF_MEMORY:
		return ExceptionType::OUT_OF_MEMORY;
	case SABOT_SQL_ERROR_PERMISSION:
		return ExceptionType::PERMISSION;
	case SABOT_SQL_ERROR_PARAMETER_NOT_RESOLVED:
		return ExceptionType::PARAMETER_NOT_RESOLVED;
	case SABOT_SQL_ERROR_PARAMETER_NOT_ALLOWED:
		return ExceptionType::PARAMETER_NOT_ALLOWED;
	case SABOT_SQL_ERROR_DEPENDENCY:
		return ExceptionType::DEPENDENCY;
	case SABOT_SQL_ERROR_HTTP:
		return ExceptionType::HTTP;
	case SABOT_SQL_ERROR_MISSING_EXTENSION:
		return ExceptionType::MISSING_EXTENSION;
	case SABOT_SQL_ERROR_AUTOLOAD:
		return ExceptionType::AUTOLOAD;
	case SABOT_SQL_ERROR_SEQUENCE:
		return ExceptionType::SEQUENCE;
	case SABOT_SQL_INVALID_CONFIGURATION:
		return ExceptionType::INVALID_CONFIGURATION;
	default:
		return ExceptionType::INVALID;
	}
}

} // namespace sabot_sql

void *sabot_sql_malloc(size_t size) {
	return malloc(size);
}

void sabot_sql_free(void *ptr) {
	free(ptr);
}

idx_t sabot_sql_vector_size() {
	return STANDARD_VECTOR_SIZE;
}

bool sabot_sql_string_is_inlined(sabot_sql_string_t string_p) {
	static_assert(sizeof(sabot_sql_string_t) == sizeof(sabot_sql::string_t),
	              "sabot_sql_string_t should have the same memory layout as sabot_sql::string_t");
	auto &string = *reinterpret_cast<sabot_sql::string_t *>(&string_p);
	return string.IsInlined();
}

uint32_t sabot_sql_string_t_length(sabot_sql_string_t string_p) {
	static_assert(sizeof(sabot_sql_string_t) == sizeof(sabot_sql::string_t),
	              "sabot_sql_string_t should have the same memory layout as sabot_sql::string_t");
	auto &string = *reinterpret_cast<sabot_sql::string_t *>(&string_p);
	return static_cast<uint32_t>(string.GetSize());
}

const char *sabot_sql_string_t_data(sabot_sql_string_t *string_p) {
	static_assert(sizeof(sabot_sql_string_t) == sizeof(sabot_sql::string_t),
	              "sabot_sql_string_t should have the same memory layout as sabot_sql::string_t");
	auto &string = *reinterpret_cast<sabot_sql::string_t *>(string_p);
	return string.GetData();
}
