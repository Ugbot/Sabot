//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/bound_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

class LogicalOperator;
struct LogicalType;

struct BoundStatement {
	unique_ptr<LogicalOperator> plan;
	vector<LogicalType> types;
	vector<string> names;
};

} // namespace sabot_sql
