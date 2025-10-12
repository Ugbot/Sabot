//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/table_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/main/table_description.hpp"

namespace sabot_sql {

class TableRelation : public Relation {
public:
	TableRelation(const shared_ptr<ClientContext> &context, unique_ptr<TableDescription> description);
	TableRelation(const shared_ptr<RelationContextWrapper> &context, unique_ptr<TableDescription> description);

	unique_ptr<TableDescription> description;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

	unique_ptr<TableRef> GetTableRef() override;

	void Update(const string &update, const string &condition = string()) override;
	void Update(vector<string> column_names, vector<unique_ptr<ParsedExpression>> &&update,
	            unique_ptr<ParsedExpression> condition = nullptr) override;
	void Delete(const string &condition = string()) override;
};

} // namespace sabot_sql
