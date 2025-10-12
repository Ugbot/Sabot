//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/aggregate_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/group_by_node.hpp"

namespace sabot_sql {

class AggregateRelation : public Relation {
public:
	SABOT_SQL_API AggregateRelation(shared_ptr<Relation> child, vector<unique_ptr<ParsedExpression>> expressions);
	SABOT_SQL_API AggregateRelation(shared_ptr<Relation> child, vector<unique_ptr<ParsedExpression>> expressions,
	                             GroupByNode groups);
	SABOT_SQL_API AggregateRelation(shared_ptr<Relation> child, vector<unique_ptr<ParsedExpression>> expressions,
	                             vector<unique_ptr<ParsedExpression>> groups);

	vector<unique_ptr<ParsedExpression>> expressions;
	GroupByNode groups;
	vector<ColumnDefinition> columns;
	shared_ptr<Relation> child;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace sabot_sql
