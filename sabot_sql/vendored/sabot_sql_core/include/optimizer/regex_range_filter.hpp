//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/regex_range_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"

namespace sabot_sql {

class Optimizer;

class RegexRangeFilter {
public:
	RegexRangeFilter() {
	}
	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
};

} // namespace sabot_sql
