//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression/bound_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

//! BoundExpression is an intermediate dummy class used by the binder. It is a ParsedExpression but holds an Expression.
//! It represents a successfully bound expression. It is used in the Binder to prevent re-binding of already bound parts
//! when dealing with subqueries.
class BoundExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_EXPRESSION;

public:
	explicit BoundExpression(unique_ptr<Expression> expr);

	unique_ptr<Expression> expr;

public:
	static unique_ptr<Expression> &GetExpression(ParsedExpression &expr);

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
};

} // namespace sabot_sql
