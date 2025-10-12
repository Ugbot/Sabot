//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/qualify_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression_binder/base_select_binder.hpp"
#include "sabot_sql/planner/expression_binder/column_alias_binder.hpp"

namespace sabot_sql {
struct SelectBindState;

//! The QUALIFY binder is responsible for binding an expression within the QUALIFY clause of a SQL statement
class QualifyBinder : public BaseSelectBinder {
public:
	QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

protected:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;

private:
	ColumnAliasBinder column_alias_binder;
};

} // namespace sabot_sql
