#include "sabot_sql/parser/expression/constant_expression.hpp"

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/value_operations/value_operations.hpp"

#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

ConstantExpression::ConstantExpression() : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT) {
}

ConstantExpression::ConstantExpression(Value val)
    : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT), value(std::move(val)) {
}

string ConstantExpression::ToString() const {
	return value.ToSQLString();
}

bool ConstantExpression::Equal(const ConstantExpression &a, const ConstantExpression &b) {
	return a.value.type() == b.value.type() && !ValueOperations::DistinctFrom(a.value, b.value);
}

hash_t ConstantExpression::Hash() const {
	return value.Hash();
}

unique_ptr<ParsedExpression> ConstantExpression::Copy() const {
	auto copy = make_uniq<ConstantExpression>(value);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace sabot_sql
