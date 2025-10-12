//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/insert_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {

//! The INSERT binder is responsible for binding expressions within the VALUES of an INSERT statement
class InsertBinder : public ExpressionBinder {
public:
	InsertBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace sabot_sql
