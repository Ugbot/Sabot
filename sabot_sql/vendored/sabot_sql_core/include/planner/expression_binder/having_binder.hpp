//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/having_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression_binder/base_select_binder.hpp"
#include "sabot_sql/planner/expression_binder/column_alias_binder.hpp"
#include "sabot_sql/common/enums/aggregate_handling.hpp"

namespace sabot_sql {

//! The HAVING binder is responsible for binding an expression within the HAVING clause of a SQL statement.
class HavingBinder : public BaseSelectBinder {
public:
	HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	             AggregateHandling aggregate_handling);

protected:
	BindResult BindLambdaReference(LambdaRefExpression &expr, idx_t depth);
	BindResult BindWindow(WindowExpression &expr, idx_t depth) override;
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;

	unique_ptr<ParsedExpression> QualifyColumnName(ColumnRefExpression &col_ref, ErrorData &error) override;

private:
	ColumnAliasBinder column_alias_binder;
	AggregateHandling aggregate_handling;
};

} // namespace sabot_sql
