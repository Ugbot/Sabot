//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/filter_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class FilterRelation : public Relation {
public:
	SABOT_SQL_API FilterRelation(shared_ptr<Relation> child, unique_ptr<ParsedExpression> condition);

	unique_ptr<ParsedExpression> condition;
	shared_ptr<Relation> child;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;

public:
	bool InheritsColumnBindings() override {
		return true;
	}
	Relation *ChildRelation() override {
		return child.get();
	}
};

} // namespace sabot_sql
