//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/order_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/result_modifier.hpp"

namespace sabot_sql {

class OrderRelation : public Relation {
public:
	SABOT_SQL_API OrderRelation(shared_ptr<Relation> child, vector<OrderByNode> orders);

	vector<OrderByNode> orders;
	shared_ptr<Relation> child;
	vector<ColumnDefinition> columns;

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
