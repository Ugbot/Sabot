//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/setop_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/common/enums/set_operation_type.hpp"

namespace sabot_sql {

class SetOpRelation : public Relation {
public:
	SetOpRelation(shared_ptr<Relation> left, shared_ptr<Relation> right, SetOperationType setop_type,
	              bool setop_all = false);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	SetOperationType setop_type;
	vector<ColumnDefinition> columns;
	bool setop_all;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace sabot_sql
