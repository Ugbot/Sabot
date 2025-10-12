//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/relation_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {

//! The relation binder is a binder used to bind expressions in the relation API
class RelationBinder : public ExpressionBinder {
public:
	RelationBinder(Binder &binder, ClientContext &context, string op);

	string op;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace sabot_sql
