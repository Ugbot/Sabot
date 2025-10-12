//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/constraints/foreign_key_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/constraint.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

class ForeignKeyConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::FOREIGN_KEY;

public:
	SABOT_SQL_API ForeignKeyConstraint(vector<string> pk_columns, vector<string> fk_columns, ForeignKeyInfo info);

	//! The set of main key table's columns
	vector<string> pk_columns;
	//! The set of foreign key table's columns
	vector<string> fk_columns;
	ForeignKeyInfo info;

public:
	SABOT_SQL_API string ToString() const override;

	SABOT_SQL_API unique_ptr<Constraint> Copy() const override;

	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);

private:
	ForeignKeyConstraint();
};

} // namespace sabot_sql
