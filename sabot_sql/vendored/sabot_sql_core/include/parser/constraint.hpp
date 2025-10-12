//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {

class Serializer;
class Deserializer;

//===--------------------------------------------------------------------===//
// Constraint Types
//===--------------------------------------------------------------------===//
enum class ConstraintType : uint8_t {
	INVALID = 0,     // invalid constraint type
	NOT_NULL = 1,    // NOT NULL constraint
	CHECK = 2,       // CHECK constraint
	UNIQUE = 3,      // UNIQUE constraint
	FOREIGN_KEY = 4, // FOREIGN KEY constraint
};

enum class ForeignKeyType : uint8_t {
	FK_TYPE_PRIMARY_KEY_TABLE = 0,   // main table
	FK_TYPE_FOREIGN_KEY_TABLE = 1,   // referencing table
	FK_TYPE_SELF_REFERENCE_TABLE = 2 // self refrencing table
};

struct ForeignKeyInfo {
	ForeignKeyType type;
	string schema;
	//! if type is FK_TYPE_FOREIGN_KEY_TABLE, means main key table, if type is FK_TYPE_PRIMARY_KEY_TABLE, means foreign
	//! key table
	string table;
	//! The set of main key table's column's index
	vector<PhysicalIndex> pk_keys;
	//! The set of foreign key table's column's index
	vector<PhysicalIndex> fk_keys;

	bool IsDeleteConstraint() const {
		return type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ||
		       type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
	}
	bool IsAppendConstraint() const {
		return type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
		       type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
	}
};

//! Constraint is the base class of any type of table constraint.
class Constraint {
public:
	SABOT_SQL_API explicit Constraint(ConstraintType type);
	SABOT_SQL_API virtual ~Constraint();

	ConstraintType type;

public:
	SABOT_SQL_API virtual string ToString() const = 0;
	SABOT_SQL_API void Print() const;

	SABOT_SQL_API virtual unique_ptr<Constraint> Copy() const = 0;

	SABOT_SQL_API virtual void Serialize(Serializer &serializer) const;
	SABOT_SQL_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast constraint to type - constraint type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast constraint to type - constraint type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};
} // namespace sabot_sql
