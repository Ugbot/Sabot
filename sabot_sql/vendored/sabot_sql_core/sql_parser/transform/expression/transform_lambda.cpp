#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/expression/lambda_expression.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformLambda(sabot_sql_libpgquery::PGLambdaFunction &node) {
	D_ASSERT(node.lhs);
	D_ASSERT(node.rhs);

	auto parameters = TransformStringList(node.lhs);
	auto rhs = TransformExpression(node.rhs);
	D_ASSERT(!parameters.empty());
	D_ASSERT(rhs);
	auto result = make_uniq<LambdaExpression>(std::move(parameters), std::move(rhs));
	SetQueryLocation(*result, node.location);
	return std::move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformSingleArrow(sabot_sql_libpgquery::PGSingleArrowFunction &node) {
	D_ASSERT(node.lhs);
	D_ASSERT(node.rhs);

	auto lhs = TransformExpression(node.lhs);
	auto rhs = TransformExpression(node.rhs);
	D_ASSERT(lhs);
	D_ASSERT(rhs);
	auto result = make_uniq<LambdaExpression>(std::move(lhs), std::move(rhs));
	SetQueryLocation(*result, node.location);
	return std::move(result);
}

} // namespace sabot_sql
