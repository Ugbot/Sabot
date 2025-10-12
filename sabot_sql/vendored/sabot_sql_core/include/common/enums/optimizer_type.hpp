//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/optimizer_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

enum class OptimizerType : uint32_t {
	INVALID = 0,
	EXPRESSION_REWRITER,
	FILTER_PULLUP,
	FILTER_PUSHDOWN,
	EMPTY_RESULT_PULLUP,
	CTE_FILTER_PUSHER,
	REGEX_RANGE,
	IN_CLAUSE,
	JOIN_ORDER,
	DELIMINATOR,
	UNNEST_REWRITER,
	UNUSED_COLUMNS,
	STATISTICS_PROPAGATION,
	COMMON_SUBEXPRESSIONS,
	COMMON_AGGREGATE,
	COLUMN_LIFETIME,
	BUILD_SIDE_PROBE_SIDE,
	LIMIT_PUSHDOWN,
	TOP_N,
	COMPRESSED_MATERIALIZATION,
	DUPLICATE_GROUPS,
	REORDER_FILTER,
	SAMPLING_PUSHDOWN,
	JOIN_FILTER_PUSHDOWN,
	EXTENSION,
	MATERIALIZED_CTE,
	SUM_REWRITER,
	LATE_MATERIALIZATION,
	CTE_INLINING,
	COMMON_SUBPLAN,
};

string OptimizerTypeToString(OptimizerType type);
OptimizerType OptimizerTypeFromString(const string &str);
vector<string> ListAllOptimizers();

} // namespace sabot_sql
