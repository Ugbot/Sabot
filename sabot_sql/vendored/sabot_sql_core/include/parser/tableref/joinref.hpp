//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/tableref/joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/join_type.hpp"
#include "sabot_sql/common/enums/joinref_type.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::JOIN;

public:
	explicit JoinRef(JoinRefType ref_type = JoinRefType::REGULAR)
	    : TableRef(TableReferenceType::JOIN), type(JoinType::INNER), ref_type(ref_type) {
	}

	//! The left hand side of the join
	unique_ptr<TableRef> left;
	//! The right hand side of the join
	unique_ptr<TableRef> right;
	//! The join condition
	unique_ptr<ParsedExpression> condition;
	//! The join type
	JoinType type;
	//! Join condition type
	JoinRefType ref_type;
	//! The set of USING columns (if any)
	vector<string> using_columns;
	//! Duplicate eliminated columns (if any)
	vector<unique_ptr<ParsedExpression>> duplicate_eliminated_columns;
	//! If we have duplicate eliminated columns if the delim is flipped
	bool delim_flipped = false;
	//! Whether or not this is an implicit cross join
	bool is_implicit = false;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a JoinRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace sabot_sql
