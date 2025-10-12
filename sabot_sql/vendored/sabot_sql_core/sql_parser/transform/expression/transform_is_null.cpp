#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/expression/operator_expression.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformNullTest(sabot_sql_libpgquery::PGNullTest &root) {
	auto arg = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(root.arg));
	if (root.argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type = (root.nulltesttype == sabot_sql_libpgquery::PG_IS_NULL)
	                               ? ExpressionType::OPERATOR_IS_NULL
	                               : ExpressionType::OPERATOR_IS_NOT_NULL;

	auto result = make_uniq<OperatorExpression>(expr_type, std::move(arg));
	SetQueryLocation(*result, root.location);
	return std::move(result);
}

} // namespace sabot_sql
