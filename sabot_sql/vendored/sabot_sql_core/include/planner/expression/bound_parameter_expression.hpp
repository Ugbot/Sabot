//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression/bound_parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/planner/bound_parameter_map.hpp"

namespace sabot_sql {

class BoundParameterExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_PARAMETER;

public:
	explicit BoundParameterExpression(const string &identifier);

	string identifier;
	shared_ptr<BoundParameterData> parameter_data;

public:
	//! Invalidate a bound parameter expression - forcing a rebind on any subsequent filters
	SABOT_SQL_API static void Invalidate(Expression &expr);
	//! Invalidate all parameters within an expression
	SABOT_SQL_API static void InvalidateRecursive(Expression &expr);

	bool IsScalar() const override;
	bool HasParameter() const override;
	bool IsFoldable() const override;

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

private:
	BoundParameterExpression(bound_parameter_map_t &global_parameter_set, string identifier, LogicalType return_type,
	                         shared_ptr<BoundParameterData> parameter_data);
};

} // namespace sabot_sql
