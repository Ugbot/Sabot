//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/common_aggregate_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/column_binding_map.hpp"
#include "sabot_sql/planner/logical_operator_visitor.hpp"

namespace sabot_sql {
//! The CommonAggregateOptimizer optimizer eliminates duplicate aggregates from aggregate nodes
class CommonAggregateOptimizer : public LogicalOperatorVisitor {
public:
	void VisitOperator(LogicalOperator &op) override;

private:
	void StandardVisitOperator(LogicalOperator &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	void ExtractCommonAggregates(LogicalAggregate &aggr);

private:
	column_binding_map_t<ColumnBinding> aggregate_map;
};
} // namespace sabot_sql
