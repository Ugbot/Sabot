#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/planner/expression/bound_constant_expression.hpp"
#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {

BindResult ExpressionBinder::BindExpression(ConstantExpression &expr, idx_t depth) {
	return BindResult(make_uniq<BoundConstantExpression>(expr.value));
}

} // namespace sabot_sql
