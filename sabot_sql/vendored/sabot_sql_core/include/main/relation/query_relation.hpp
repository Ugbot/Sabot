//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/query_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/query_node.hpp"

namespace sabot_sql {
class SelectStatement;

class QueryRelation : public Relation {
public:
	QueryRelation(const shared_ptr<ClientContext> &context, unique_ptr<SelectStatement> select_stmt, string alias,
	              const string &query = "");
	~QueryRelation() override;

	unique_ptr<SelectStatement> select_stmt;
	string query;
	string alias;
	vector<ColumnDefinition> columns;

public:
	static unique_ptr<SelectStatement> ParseStatement(ClientContext &context, const string &query, const string &error);
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
	BoundStatement Bind(Binder &binder) override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

private:
	unique_ptr<SelectStatement> GetSelectStatement();
};

} // namespace sabot_sql
