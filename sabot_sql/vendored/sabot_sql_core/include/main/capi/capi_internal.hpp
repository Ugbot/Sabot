//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/capi/capi_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql.h"
#include "sabot_sql.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/main/appender.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/planner/expression/bound_parameter_data.hpp"
#include "sabot_sql/main/db_instance_cache.hpp"

#include <cstring>
#include <cassert>

#ifdef _WIN32
#ifndef strdup
#define strdup _strdup
#endif
#endif

namespace sabot_sql {

struct DBInstanceCacheWrapper {
	unique_ptr<DBInstanceCache> instance_cache;
};

struct DatabaseWrapper {
	shared_ptr<SabotSQL> database;
};

struct CClientContextWrapper {
	explicit CClientContextWrapper(ClientContext &context) : context(context) {};
	ClientContext &context;
};

struct CClientArrowOptionsWrapper {
	explicit CClientArrowOptionsWrapper(ClientProperties &properties) : properties(properties) {};
	ClientProperties properties;
};

struct PreparedStatementWrapper {
	//! Map of name -> values
	case_insensitive_map_t<BoundParameterData> values;
	unique_ptr<PreparedStatement> statement;
};

struct ExtractStatementsWrapper {
	vector<unique_ptr<SQLStatement>> statements;
	string error;
};

struct PendingStatementWrapper {
	unique_ptr<PendingQueryResult> statement;
	bool allow_streaming;
};

struct ArrowResultWrapper {
	unique_ptr<MaterializedQueryResult> result;
	unique_ptr<DataChunk> current_chunk;
};

struct AppenderWrapper {
	unique_ptr<BaseAppender> appender;
	ErrorData error_data;
};

struct TableDescriptionWrapper {
	unique_ptr<TableDescription> description;
	string error;
};

struct ErrorDataWrapper {
	ErrorData error_data;
};

struct ExpressionWrapper {
	unique_ptr<Expression> expr;
};

enum class CAPIResultSetType : uint8_t {
	CAPI_RESULT_TYPE_NONE = 0,
	CAPI_RESULT_TYPE_MATERIALIZED,
	CAPI_RESULT_TYPE_STREAMING,
	CAPI_RESULT_TYPE_DEPRECATED
};

struct SabotSQLResultData {
	//! The underlying query result
	unique_ptr<QueryResult> result;
	// Results can only use either the new API or the old API, not a mix of the two
	// They start off as "none" and switch to one or the other when an API method is used
	CAPIResultSetType result_set_type;
};

sabot_sql_type LogicalTypeIdToC(const LogicalTypeId type);
LogicalTypeId LogicalTypeIdFromC(const sabot_sql_type type);
idx_t GetCTypeSize(const sabot_sql_type type);
sabot_sql_statement_type StatementTypeToC(const StatementType type);
sabot_sql_error_type ErrorTypeToC(const ExceptionType type);
ExceptionType ErrorTypeFromC(const sabot_sql_error_type type);

sabot_sql_state SabotSQLTranslateResult(unique_ptr<QueryResult> result, sabot_sql_result *out);
bool DeprecatedMaterializeResult(sabot_sql_result *result);

} // namespace sabot_sql
