//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/subquery_type.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"

namespace sabot_sql {

//! Represents a subquery
class SubqueryExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::SUBQUERY;

public:
	SubqueryExpression();

	//! The actual subquery
	unique_ptr<SelectStatement> subquery;
	//! The subquery type
	SubqueryType subquery_type;
	//! the child expression to compare with (in case of IN, ANY, ALL operators, empty for EXISTS queries and scalar
	//! subquery)
	unique_ptr<ParsedExpression> child;
	//! The comparison type of the child expression with the subquery (in case of ANY, ALL operators), empty otherwise
	ExpressionType comparison_type;

public:
	bool HasSubquery() const override {
		return true;
	}
	bool IsScalar() const override {
		return false;
	}

	string ToString() const override;

	static bool Equal(const SubqueryExpression &a, const SubqueryExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
