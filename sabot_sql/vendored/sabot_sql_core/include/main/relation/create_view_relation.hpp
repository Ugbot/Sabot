//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/create_view_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class CreateViewRelation : public Relation {
public:
	CreateViewRelation(shared_ptr<Relation> child, string view_name, bool replace, bool temporary);
	CreateViewRelation(shared_ptr<Relation> child, string schema_name, string view_name, bool replace, bool temporary);

	shared_ptr<Relation> child;
	string schema_name;
	string view_name;
	bool replace;
	bool temporary;
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
