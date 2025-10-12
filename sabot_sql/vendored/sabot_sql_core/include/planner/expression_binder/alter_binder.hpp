//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/alter_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {
class TableCatalogEntry;

//! The AlterBinder binds expressions in ALTER statements.
class AlterBinder : public ExpressionBinder {
public:
	AlterBinder(Binder &binder, ClientContext &context, TableCatalogEntry &table, vector<LogicalIndex> &bound_columns,
	            LogicalType target_type);

protected:
	BindResult BindLambdaReference(LambdaRefExpression &expr, idx_t depth);
	BindResult BindColumnReference(ColumnRefExpression &expr, idx_t depth);
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	TableCatalogEntry &table;
	vector<LogicalIndex> &bound_columns;
};

} // namespace sabot_sql
