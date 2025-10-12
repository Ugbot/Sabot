//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/constraints/check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/string_util.hpp"
#include "sabot_sql/parser/constraint.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class CheckConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::CHECK;

public:
	SABOT_SQL_API explicit CheckConstraint(unique_ptr<ParsedExpression> expression);

	unique_ptr<ParsedExpression> expression;

public:
	SABOT_SQL_API string ToString() const override;

	SABOT_SQL_API unique_ptr<Constraint> Copy() const override;

	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
