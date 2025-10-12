//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/projection_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class ProjectionRelation : public Relation {
public:
	SABOT_SQL_API ProjectionRelation(shared_ptr<Relation> child, vector<unique_ptr<ParsedExpression>> expressions,
	                              vector<string> aliases);

	vector<unique_ptr<ParsedExpression>> expressions;
	vector<ColumnDefinition> columns;
	shared_ptr<Relation> child;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace sabot_sql
