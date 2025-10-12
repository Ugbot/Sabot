//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/constraints/not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/constraint.hpp"

namespace sabot_sql {

class NotNullConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::NOT_NULL;

public:
	SABOT_SQL_API explicit NotNullConstraint(LogicalIndex index);
	SABOT_SQL_API ~NotNullConstraint() override;

	//! Column index this constraint pertains to
	LogicalIndex index;

public:
	SABOT_SQL_API string ToString() const override;

	SABOT_SQL_API unique_ptr<Constraint> Copy() const override;

	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
