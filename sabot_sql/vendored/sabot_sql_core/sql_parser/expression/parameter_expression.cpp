#include "sabot_sql/parser/expression/parameter_expression.hpp"

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/to_string.hpp"

#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

ParameterExpression::ParameterExpression()
    : ParsedExpression(ExpressionType::VALUE_PARAMETER, ExpressionClass::PARAMETER) {
}

string ParameterExpression::ToString() const {
	return "$" + identifier;
}

unique_ptr<ParsedExpression> ParameterExpression::Copy() const {
	auto copy = make_uniq<ParameterExpression>();
	copy->identifier = identifier;
	copy->CopyProperties(*this);
	return std::move(copy);
}

bool ParameterExpression::Equal(const ParameterExpression &a, const ParameterExpression &b) {
	return StringUtil::CIEquals(a.identifier, b.identifier);
}

hash_t ParameterExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	return CombineHash(sabot_sql::Hash(identifier.c_str(), identifier.size()), result);
}

} // namespace sabot_sql
