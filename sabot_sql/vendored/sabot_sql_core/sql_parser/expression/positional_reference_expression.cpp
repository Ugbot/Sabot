#include "sabot_sql/parser/expression/positional_reference_expression.hpp"

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/to_string.hpp"

#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

PositionalReferenceExpression::PositionalReferenceExpression()
    : ParsedExpression(ExpressionType::POSITIONAL_REFERENCE, ExpressionClass::POSITIONAL_REFERENCE) {
}

PositionalReferenceExpression::PositionalReferenceExpression(idx_t index)
    : ParsedExpression(ExpressionType::POSITIONAL_REFERENCE, ExpressionClass::POSITIONAL_REFERENCE), index(index) {
}

string PositionalReferenceExpression::ToString() const {
	return "#" + to_string(index);
}

bool PositionalReferenceExpression::Equal(const PositionalReferenceExpression &a,
                                          const PositionalReferenceExpression &b) {
	return a.index == b.index;
}

unique_ptr<ParsedExpression> PositionalReferenceExpression::Copy() const {
	auto copy = make_uniq<PositionalReferenceExpression>(index);
	copy->CopyProperties(*this);
	return std::move(copy);
}

hash_t PositionalReferenceExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	return CombineHash(sabot_sql::Hash(index), result);
}

} // namespace sabot_sql
