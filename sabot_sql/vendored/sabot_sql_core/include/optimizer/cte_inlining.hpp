//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/cte_inlining.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/insertion_order_preserving_map.hpp"
#include "sabot_sql/planner/bound_parameter_map.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/planner/expression/list.hpp"
#include "sabot_sql/planner/expression_iterator.hpp"
#include "sabot_sql/planner/operator/list.hpp"

namespace sabot_sql {

class LogicalOperator;
class Optimizer;
struct BoundParameterData;

class CTEInlining {
public:
	explicit CTEInlining(Optimizer &optimizer);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	static bool EndsInAggregateOrDistinct(const LogicalOperator &op);

private:
	void TryInlining(unique_ptr<LogicalOperator> &op);
	bool Inline(unique_ptr<LogicalOperator> &op, LogicalOperator &materialized_cte, bool requires_copy = true);

private:
	//! The optimizer
	Optimizer &optimizer;

	optional_ptr<bound_parameter_map_t> parameter_data;
};

class PreventInlining : public LogicalOperatorVisitor {
public:
	PreventInlining() : prevent_inlining(false) {};

	void VisitOperator(LogicalOperator &op) override;
	bool prevent_inlining;

	void VisitExpression(unique_ptr<Expression> *expression) override;
};

} // namespace sabot_sql
