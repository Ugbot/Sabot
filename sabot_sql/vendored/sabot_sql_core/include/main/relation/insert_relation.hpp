//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/insert_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class InsertRelation : public Relation {
public:
	InsertRelation(shared_ptr<Relation> child, string schema_name, string table_name);

	shared_ptr<Relation> child;
	string schema_name;
	string table_name;
	vector<ColumnDefinition> columns;

public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace sabot_sql
