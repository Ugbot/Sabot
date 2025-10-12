//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/common_subplan_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

class Optimizer;
class LogicalOperator;

//! The CommonSubplanOptimizer optimizer detects common subplans, and converts them to refs of a materialized CTE
class CommonSubplanOptimizer {
public:
	explicit CommonSubplanOptimizer(Optimizer &optimizer);

public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! The optimizer
	Optimizer &optimizer;
};

} // namespace sabot_sql
