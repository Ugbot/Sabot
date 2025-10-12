//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/subquery_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"

namespace sabot_sql {

class SubqueryRelation : public Relation {
public:
	SubqueryRelation(shared_ptr<Relation> child, const string &alias);
	shared_ptr<Relation> child;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

public:
	bool InheritsColumnBindings() override {
		return child->InheritsColumnBindings();
	}
	Relation *ChildRelation() override {
		return child->ChildRelation();
	}
};

} // namespace sabot_sql
