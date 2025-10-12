//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression/default_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {
//! Represents the default value of a column
class DefaultExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::DEFAULT;

public:
	DefaultExpression();

public:
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
