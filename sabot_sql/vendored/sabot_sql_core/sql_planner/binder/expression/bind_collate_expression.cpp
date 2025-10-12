#include "sabot_sql/parser/expression/collate_expression.hpp"
#include "sabot_sql/planner/expression/bound_constant_expression.hpp"
#include "sabot_sql/planner/expression/bound_parameter_expression.hpp"
#include "sabot_sql/planner/expression_binder.hpp"

namespace sabot_sql {

BindResult ExpressionBinder::BindExpression(CollateExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	auto error = Bind(expr.child, depth);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	auto &child = BoundExpression::GetExpression(*expr.child);
	if (child->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (child->return_type.id() != LogicalTypeId::VARCHAR) {
		throw BinderException("collations are only supported for type varchar");
	}
	// Validate the collation, but don't use it
	auto collation_test = make_uniq_base<Expression, BoundConstantExpression>(Value(child->return_type));
	auto collation_type = LogicalType::VARCHAR_COLLATION(expr.collation);
	PushCollation(context, collation_test, collation_type);
	child->return_type = collation_type;
	return BindResult(std::move(child));
}

} // namespace sabot_sql
