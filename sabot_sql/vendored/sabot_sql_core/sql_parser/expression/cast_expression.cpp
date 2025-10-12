#include "sabot_sql/parser/expression/cast_expression.hpp"

#include "sabot_sql/common/exception.hpp"

#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

CastExpression::CastExpression(LogicalType target, unique_ptr<ParsedExpression> child, bool try_cast_p)
    : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(std::move(target)),
      try_cast(try_cast_p) {
	D_ASSERT(child);
	this->child = std::move(child);
}

CastExpression::CastExpression() : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST) {
}

string CastExpression::ToString() const {
	return ToString<CastExpression, ParsedExpression>(*this);
}

bool CastExpression::Equal(const CastExpression &a, const CastExpression &b) {
	if (!a.child->Equals(*b.child)) {
		return false;
	}
	if (a.cast_type != b.cast_type) {
		return false;
	}
	if (a.try_cast != b.try_cast) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CastExpression::Copy() const {
	auto copy = make_uniq<CastExpression>(cast_type, child->Copy(), try_cast);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace sabot_sql
