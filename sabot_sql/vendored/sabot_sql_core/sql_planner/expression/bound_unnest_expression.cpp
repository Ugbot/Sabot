#include "sabot_sql/planner/expression/bound_unnest_expression.hpp"

#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

BoundUnnestExpression::BoundUnnestExpression(LogicalType return_type)
    : Expression(ExpressionType::BOUND_UNNEST, ExpressionClass::BOUND_UNNEST, std::move(return_type)) {
}

bool BoundUnnestExpression::IsFoldable() const {
	return false;
}

string BoundUnnestExpression::ToString() const {
	return "UNNEST(" + child->ToString() + ")";
}

hash_t BoundUnnestExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, sabot_sql::Hash("unnest"));
}

bool BoundUnnestExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundUnnestExpression>();
	if (!Expression::Equals(*child, *other.child)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundUnnestExpression::Copy() const {
	auto copy = make_uniq<BoundUnnestExpression>(return_type);
	copy->child = child->Copy();
	return std::move(copy);
}

} // namespace sabot_sql
