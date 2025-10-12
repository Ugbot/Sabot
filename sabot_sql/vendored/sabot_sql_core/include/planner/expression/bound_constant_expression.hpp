//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression/bound_constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

class BoundConstantExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CONSTANT;

public:
	explicit BoundConstantExpression(Value value);

	Value value;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
