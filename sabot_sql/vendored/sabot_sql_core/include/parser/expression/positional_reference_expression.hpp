//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression/positional_reference_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {
class PositionalReferenceExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::POSITIONAL_REFERENCE;

public:
	SABOT_SQL_API explicit PositionalReferenceExpression(idx_t index);

	idx_t index;

public:
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	static bool Equal(const PositionalReferenceExpression &a, const PositionalReferenceExpression &b);
	unique_ptr<ParsedExpression> Copy() const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	PositionalReferenceExpression();
};
} // namespace sabot_sql
