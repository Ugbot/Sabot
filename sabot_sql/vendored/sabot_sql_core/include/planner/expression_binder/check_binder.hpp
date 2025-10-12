//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/check_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/planner/expression_binder.hpp"
#include "sabot_sql/common/index_map.hpp"
#include "sabot_sql/parser/column_list.hpp"

namespace sabot_sql {
//! The CHECK binder is responsible for binding an expression within a CHECK constraint
class CheckBinder : public ExpressionBinder {
public:
	CheckBinder(Binder &binder, ClientContext &context, string table, const ColumnList &columns,
	            physical_index_set_t &bound_columns);

	string table;
	const ColumnList &columns;
	physical_index_set_t &bound_columns;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	BindResult BindCheckColumn(ColumnRefExpression &expr);

	string UnsupportedAggregateMessage() override;
};

} // namespace sabot_sql
