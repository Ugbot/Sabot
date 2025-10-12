#include "sabot_sql/parser/expression/case_expression.hpp"
#include "sabot_sql/parser/expression/comparison_expression.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformCase(sabot_sql_libpgquery::PGCaseExpr &root) {
	auto case_node = make_uniq<CaseExpression>();
	auto root_arg = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(root.arg));
	for (auto cell = root.args->head; cell != nullptr; cell = cell->next) {
		CaseCheck case_check;

		auto w = PGPointerCast<sabot_sql_libpgquery::PGCaseWhen>(cell->data.ptr_value);
		auto test_raw = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(w->expr));
		unique_ptr<ParsedExpression> test;
		if (root_arg) {
			case_check.when_expr =
			    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, root_arg->Copy(), std::move(test_raw));
		} else {
			case_check.when_expr = std::move(test_raw);
		}
		case_check.then_expr = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(w->result));
		case_node->case_checks.push_back(std::move(case_check));
	}

	if (root.defresult) {
		case_node->else_expr = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(root.defresult));
	} else {
		case_node->else_expr = make_uniq<ConstantExpression>(Value(LogicalType::SQLNULL));
	}
	SetQueryLocation(*case_node, root.location);
	return std::move(case_node);
}

} // namespace sabot_sql
