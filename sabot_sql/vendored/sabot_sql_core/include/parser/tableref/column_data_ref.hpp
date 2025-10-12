//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/tableref/column_data_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/common/optionally_owned_ptr.hpp"
#include "sabot_sql/common/types/column/column_data_collection.hpp"

namespace sabot_sql {

//! Represents a TableReference to a materialized result
class ColumnDataRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::COLUMN_DATA;

public:
	explicit ColumnDataRef(optionally_owned_ptr<ColumnDataCollection> collection_p,
	                       vector<string> expected_names = vector<string>());

public:
	//! The set of expected names
	vector<string> expected_names;
	//! (Optionally) the owned collection
	optionally_owned_ptr<ColumnDataCollection> collection;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a ColumnDataRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace sabot_sql
