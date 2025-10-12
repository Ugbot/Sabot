//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression/bound_conjunction_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

class BoundConjunctionExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_CONJUNCTION;

public:
	explicit BoundConjunctionExpression(ExpressionType type);
	BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

	vector<unique_ptr<Expression>> children;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	bool PropagatesNullValues() const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
