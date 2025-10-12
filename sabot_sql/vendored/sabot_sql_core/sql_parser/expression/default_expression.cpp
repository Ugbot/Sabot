#include "sabot_sql/parser/expression/default_expression.hpp"

#include "sabot_sql/common/exception.hpp"

#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

DefaultExpression::DefaultExpression() : ParsedExpression(ExpressionType::VALUE_DEFAULT, ExpressionClass::DEFAULT) {
}

string DefaultExpression::ToString() const {
	return "DEFAULT";
}

unique_ptr<ParsedExpression> DefaultExpression::Copy() const {
	auto copy = make_uniq<DefaultExpression>();
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace sabot_sql
