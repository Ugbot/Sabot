//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression/between_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class BetweenExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BETWEEN;

public:
	SABOT_SQL_API BetweenExpression(unique_ptr<ParsedExpression> input, unique_ptr<ParsedExpression> lower,
	                             unique_ptr<ParsedExpression> upper);

	unique_ptr<ParsedExpression> input;
	unique_ptr<ParsedExpression> lower;
	unique_ptr<ParsedExpression> upper;

public:
	string ToString() const override;

	static bool Equal(const BetweenExpression &a, const BetweenExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		return "(" + entry.input->ToString() + " BETWEEN " + entry.lower->ToString() + " AND " +
		       entry.upper->ToString() + ")";
	}

private:
	BetweenExpression();
};
} // namespace sabot_sql
