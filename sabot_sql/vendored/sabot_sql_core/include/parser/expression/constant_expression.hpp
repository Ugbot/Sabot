//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression/constant_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

//! ConstantExpression represents a constant value in the query
class ConstantExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::CONSTANT;

public:
	SABOT_SQL_API explicit ConstantExpression(Value val);

	//! The constant value referenced
	Value value;

public:
	string ToString() const override;

	static bool Equal(const ConstantExpression &a, const ConstantExpression &b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	ConstantExpression();
};

} // namespace sabot_sql
