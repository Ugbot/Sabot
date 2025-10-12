//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/create_table_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class CreateTableRelation : public Relation {
public:
	CreateTableRelation(shared_ptr<Relation> child, string schema_name, string table_name, bool temporary,
	                    OnCreateConflict on_conflict);

	shared_ptr<Relation> child;
	string schema_name;
	string table_name;
	vector<ColumnDefinition> columns;
	bool temporary;
	OnCreateConflict on_conflict;

public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace sabot_sql
