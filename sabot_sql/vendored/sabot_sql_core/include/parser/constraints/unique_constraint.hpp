//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/constraints/unique_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enum_util.hpp"
#include "sabot_sql/common/enums/index_constraint_type.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/parser/column_list.hpp"
#include "sabot_sql/parser/constraint.hpp"

namespace sabot_sql {

class UniqueConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::UNIQUE;

public:
	SABOT_SQL_API UniqueConstraint(const LogicalIndex index, const bool is_primary_key);
	SABOT_SQL_API UniqueConstraint(const LogicalIndex index, string column_name, const bool is_primary_key);
	SABOT_SQL_API UniqueConstraint(vector<string> columns, const bool is_primary_key);

public:
	SABOT_SQL_API string ToString() const override;
	SABOT_SQL_API unique_ptr<Constraint> Copy() const override;
	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);

	//! Returns true, if the constraint is a PRIMARY KEY constraint.
	bool IsPrimaryKey() const;
	//! Returns true, if the constraint is defined on a single column.
	bool HasIndex() const;
	//! Returns the column index on which the constraint is defined.
	LogicalIndex GetIndex() const;
	//! Sets the column index of the constraint.
	void SetIndex(const LogicalIndex new_index);
	//! Returns a constant reference to the column names on which the constraint is defined.
	const vector<string> &GetColumnNames() const;
	//! Returns a mutable reference to the column names on which the constraint is defined.
	vector<string> &GetColumnNamesMutable();
	//! Returns the column indexes on which the constraint is defined.
	vector<LogicalIndex> GetLogicalIndexes(const ColumnList &columns) const;
	//! Get the name of the constraint.
	string GetName(const string &table_name) const;

private:
	UniqueConstraint();

#ifdef SABOT_SQL_API_1_0
private:
#else
public:
#endif

	//! The indexed column of the constraint. Only used for single-column constraints, invalid otherwise.
	LogicalIndex index;
	//! The names of the columns on which this constraint is defined. Only set if the index field is not set.
	vector<string> columns;
	//! Whether this is a PRIMARY KEY constraint, or a UNIQUE constraint.
	bool is_primary_key;
};

} // namespace sabot_sql
