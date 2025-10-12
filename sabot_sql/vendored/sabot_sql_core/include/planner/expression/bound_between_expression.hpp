//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression/bound_between_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

class BoundBetweenExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_BETWEEN;

public:
	BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower, unique_ptr<Expression> upper,
	                       bool lower_inclusive, bool upper_inclusive);

	unique_ptr<Expression> input;
	unique_ptr<Expression> lower;
	unique_ptr<Expression> upper;
	bool lower_inclusive;
	bool upper_inclusive;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

public:
	ExpressionType LowerComparisonType() const {
		return lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO : ExpressionType::COMPARE_GREATERTHAN;
	}
	ExpressionType UpperComparisonType() const {
		return upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN;
	}

private:
	BoundBetweenExpression();
};
} // namespace sabot_sql
