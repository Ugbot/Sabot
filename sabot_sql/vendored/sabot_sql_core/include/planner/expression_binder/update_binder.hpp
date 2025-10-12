//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/update_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {

//! The UPDATE binder is responsible for binding an expression within an UPDATE statement
class UpdateBinder : public ExpressionBinder {
public:
	UpdateBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace sabot_sql
