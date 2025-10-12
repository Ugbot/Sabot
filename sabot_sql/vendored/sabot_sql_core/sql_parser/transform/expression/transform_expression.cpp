#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/expression/default_expression.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<ParsedExpression> Transformer::TransformResTarget(sabot_sql_libpgquery::PGResTarget &root) {
	auto expr = TransformExpression(root.val);
	if (!expr) {
		return nullptr;
	}
	if (root.name) {
		expr->SetAlias(root.name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformNamedArg(sabot_sql_libpgquery::PGNamedArgExpr &root) {

	auto expr = TransformExpression(PGPointerCast<sabot_sql_libpgquery::PGNode>(root.arg));
	if (root.name) {
		expr->SetAlias(root.name);
	}
	return expr;
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(sabot_sql_libpgquery::PGNode &node) {

	auto stack_checker = StackCheck();

	switch (node.type) {
	case sabot_sql_libpgquery::T_PGColumnRef:
		return TransformColumnRef(PGCast<sabot_sql_libpgquery::PGColumnRef>(node));
	case sabot_sql_libpgquery::T_PGAConst:
		return TransformConstant(PGCast<sabot_sql_libpgquery::PGAConst>(node));
	case sabot_sql_libpgquery::T_PGAExpr:
		return TransformAExpr(PGCast<sabot_sql_libpgquery::PGAExpr>(node));
	case sabot_sql_libpgquery::T_PGFuncCall:
		return TransformFuncCall(PGCast<sabot_sql_libpgquery::PGFuncCall>(node));
	case sabot_sql_libpgquery::T_PGBoolExpr:
		return TransformBoolExpr(PGCast<sabot_sql_libpgquery::PGBoolExpr>(node));
	case sabot_sql_libpgquery::T_PGTypeCast:
		return TransformTypeCast(PGCast<sabot_sql_libpgquery::PGTypeCast>(node));
	case sabot_sql_libpgquery::T_PGCaseExpr:
		return TransformCase(PGCast<sabot_sql_libpgquery::PGCaseExpr>(node));
	case sabot_sql_libpgquery::T_PGSubLink:
		return TransformSubquery(PGCast<sabot_sql_libpgquery::PGSubLink>(node));
	case sabot_sql_libpgquery::T_PGCoalesceExpr:
		return TransformCoalesce(PGCast<sabot_sql_libpgquery::PGAExpr>(node));
	case sabot_sql_libpgquery::T_PGNullTest:
		return TransformNullTest(PGCast<sabot_sql_libpgquery::PGNullTest>(node));
	case sabot_sql_libpgquery::T_PGResTarget:
		return TransformResTarget(PGCast<sabot_sql_libpgquery::PGResTarget>(node));
	case sabot_sql_libpgquery::T_PGParamRef:
		return TransformParamRef(PGCast<sabot_sql_libpgquery::PGParamRef>(node));
	case sabot_sql_libpgquery::T_PGNamedArgExpr:
		return TransformNamedArg(PGCast<sabot_sql_libpgquery::PGNamedArgExpr>(node));
	case sabot_sql_libpgquery::T_PGSQLValueFunction:
		return TransformSQLValueFunction(PGCast<sabot_sql_libpgquery::PGSQLValueFunction>(node));
	case sabot_sql_libpgquery::T_PGSetToDefault:
		return make_uniq<DefaultExpression>();
	case sabot_sql_libpgquery::T_PGCollateClause:
		return TransformCollateExpr(PGCast<sabot_sql_libpgquery::PGCollateClause>(node));
	case sabot_sql_libpgquery::T_PGIntervalConstant:
		return TransformInterval(PGCast<sabot_sql_libpgquery::PGIntervalConstant>(node));
	case sabot_sql_libpgquery::T_PGLambdaFunction:
		return TransformLambda(PGCast<sabot_sql_libpgquery::PGLambdaFunction>(node));
	case sabot_sql_libpgquery::T_PGSingleArrowFunction:
		return TransformSingleArrow(PGCast<sabot_sql_libpgquery::PGSingleArrowFunction>(node));
	case sabot_sql_libpgquery::T_PGAIndirection:
		return TransformArrayAccess(PGCast<sabot_sql_libpgquery::PGAIndirection>(node));
	case sabot_sql_libpgquery::T_PGPositionalReference:
		return TransformPositionalReference(PGCast<sabot_sql_libpgquery::PGPositionalReference>(node));
	case sabot_sql_libpgquery::T_PGGroupingFunc:
		return TransformGroupingFunction(PGCast<sabot_sql_libpgquery::PGGroupingFunc>(node));
	case sabot_sql_libpgquery::T_PGAStar:
		return TransformStarExpression(PGCast<sabot_sql_libpgquery::PGAStar>(node));
	case sabot_sql_libpgquery::T_PGBooleanTest:
		return TransformBooleanTest(PGCast<sabot_sql_libpgquery::PGBooleanTest>(node));
	case sabot_sql_libpgquery::T_PGMultiAssignRef:
		return TransformMultiAssignRef(PGCast<sabot_sql_libpgquery::PGMultiAssignRef>(node));
	default:
		throw NotImplementedException("Expression type %s (%d)", NodetypeToString(node.type), (int)node.type);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformExpression(optional_ptr<sabot_sql_libpgquery::PGNode> node) {
	if (!node) {
		return nullptr;
	}
	return TransformExpression(*node);
}

void Transformer::TransformExpressionList(sabot_sql_libpgquery::PGList &list,
                                          vector<unique_ptr<ParsedExpression>> &result) {
	for (auto node = list.head; node != nullptr; node = node->next) {
		auto target = PGPointerCast<sabot_sql_libpgquery::PGNode>(node->data.ptr_value);

		auto expr = TransformExpression(*target);
		result.push_back(std::move(expr));
	}
}

} // namespace sabot_sql
