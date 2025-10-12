//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/deliminator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/column_binding_replacer.hpp"

namespace sabot_sql {

//! The EmptyResultPullup Optimizer traverses the logical operator tree and Pulls up empty operators when possible
class EmptyResultPullup : LogicalOperatorVisitor {
public:
	EmptyResultPullup() {
	}

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	unique_ptr<LogicalOperator> PullUpEmptyJoinChildren(unique_ptr<LogicalOperator> op);
};

} // namespace sabot_sql
