//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/join_filter_pushdown_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/logical_operator_visitor.hpp"
#include "sabot_sql/planner/column_binding_map.hpp"

namespace sabot_sql {
class Optimizer;
struct JoinFilterPushdownColumn;
struct PushdownFilterTarget;

//! The JoinFilterPushdownOptimizer links comparison joins to data sources to enable dynamic execution-time filter
//! pushdown
class JoinFilterPushdownOptimizer : public LogicalOperatorVisitor {
public:
	explicit JoinFilterPushdownOptimizer(Optimizer &optimizer);

public:
	void VisitOperator(LogicalOperator &op) override;
	static void GetPushdownFilterTargets(LogicalOperator &op, vector<JoinFilterPushdownColumn> columns,
	                                     vector<PushdownFilterTarget> &targets);

private:
	void GenerateJoinFilters(LogicalComparisonJoin &join);

private:
	Optimizer &optimizer;
};
} // namespace sabot_sql
