#include "sabot_sql/parser/expression/operator_expression.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

// COALESCE(a,b,c) returns the first argument that is NOT NULL, so
// rewrite into CASE(a IS NOT NULL, a, CASE(b IS NOT NULL, b, c))
unique_ptr<ParsedExpression> Transformer::TransformCoalesce(sabot_sql_libpgquery::PGAExpr &root) {
	auto coalesce_args = PGPointerCast<sabot_sql_libpgquery::PGList>(root.lexpr);
	D_ASSERT(coalesce_args->length > 0); // parser ensures this already

	auto coalesce_op = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
	for (auto cell = coalesce_args->head; cell; cell = cell->next) {
		// get the value of the COALESCE
		auto value_expr = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(cell->data.ptr_value));
		coalesce_op->children.push_back(std::move(value_expr));
	}
	return std::move(coalesce_op);
}

} // namespace sabot_sql
