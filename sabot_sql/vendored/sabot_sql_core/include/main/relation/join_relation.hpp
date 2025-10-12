//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/join_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/common/enums/joinref_type.hpp"

namespace sabot_sql {

class JoinRelation : public Relation {
public:
	SABOT_SQL_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right,
	                        unique_ptr<ParsedExpression> condition, JoinType type,
	                        JoinRefType join_ref_type = JoinRefType::REGULAR);
	SABOT_SQL_API JoinRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, vector<string> using_columns,
	                        JoinType type, JoinRefType join_ref_type = JoinRefType::REGULAR);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	unique_ptr<ParsedExpression> condition;
	vector<string> using_columns;
	JoinType join_type;
	JoinRefType join_ref_type;
	vector<ColumnDefinition> columns;

	vector<unique_ptr<ParsedExpression>> duplicate_eliminated_columns;
	bool delim_flipped = false;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace sabot_sql
