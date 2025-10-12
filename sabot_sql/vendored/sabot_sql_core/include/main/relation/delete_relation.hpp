//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/delete_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class DeleteRelation : public Relation {
public:
	DeleteRelation(shared_ptr<ClientContextWrapper> &context, unique_ptr<ParsedExpression> condition,
	               string catalog_name, string schema_name, string table_name);

	vector<ColumnDefinition> columns;
	unique_ptr<ParsedExpression> condition;
	string catalog_name;
	string schema_name;
	string table_name;

public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace sabot_sql
