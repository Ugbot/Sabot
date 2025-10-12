#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/expression/operator_expression.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformGroupingFunction(sabot_sql_libpgquery::PGGroupingFunc &grouping) {
	auto op = make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION);
	for (auto node = grouping.args->head; node; node = node->next) {
		auto n = PGPointerCast<sabot_sql_libpgquery::PGNode>(node->data.ptr_value);
		op->children.push_back(TransformExpression(n));
	}
	SetQueryLocation(*op, grouping.location);
	return std::move(op);
}

} // namespace sabot_sql
