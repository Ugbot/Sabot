//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/materialized_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class MaterializedRelation : public Relation {
public:
	MaterializedRelation(const shared_ptr<ClientContext> &context, unique_ptr<ColumnDataCollection> &&collection,
	                     vector<string> names, string alias = "materialized");
	vector<ColumnDefinition> columns;
	string alias;
	shared_ptr<ColumnDataCollection> collection;

public:
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
	unique_ptr<TableRef> GetTableRef() override;
	unique_ptr<QueryNode> GetQueryNode() override;
};

} // namespace sabot_sql
