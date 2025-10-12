//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/lateral_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression_binder.hpp"
#include "sabot_sql/planner/binder.hpp"

namespace sabot_sql {

class ColumnAliasBinder;

//! The LATERAL binder is responsible for binding an expression within a LATERAL join
class LateralBinder : public ExpressionBinder {
public:
	LateralBinder(Binder &binder, ClientContext &context);

	bool HasCorrelatedColumns() const {
		return !correlated_columns.empty();
	}

	static void ReduceExpressionDepth(LogicalOperator &op, const CorrelatedColumns &info);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);
	void ExtractCorrelatedColumns(Expression &expr);

private:
	CorrelatedColumns correlated_columns;
};

} // namespace sabot_sql
