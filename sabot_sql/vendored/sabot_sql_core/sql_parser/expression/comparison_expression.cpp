#include "sabot_sql/parser/expression/comparison_expression.hpp"

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/expression/cast_expression.hpp"

#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

ComparisonExpression::ComparisonExpression(ExpressionType type) : ParsedExpression(type, ExpressionClass::COMPARISON) {
}

ComparisonExpression::ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::COMPARISON), left(std::move(left)), right(std::move(right)) {
}

string ComparisonExpression::ToString() const {
	return ToString<ComparisonExpression, ParsedExpression>(*this);
}

bool ComparisonExpression::Equal(const ComparisonExpression &a, const ComparisonExpression &b) {
	if (!a.left->Equals(*b.left)) {
		return false;
	}
	if (!a.right->Equals(*b.right)) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> ComparisonExpression::Copy() const {
	auto copy = make_uniq<ComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace sabot_sql
