//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/relation/cross_product_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/relation.hpp"
#include "sabot_sql/common/enums/joinref_type.hpp"

namespace sabot_sql {

class CrossProductRelation : public Relation {
public:
	SABOT_SQL_API CrossProductRelation(shared_ptr<Relation> left, shared_ptr<Relation> right,
	                                JoinRefType join_ref_type = JoinRefType::CROSS);

	shared_ptr<Relation> left;
	shared_ptr<Relation> right;
	JoinRefType ref_type;
	vector<ColumnDefinition> columns;

public:
	unique_ptr<QueryNode> GetQueryNode() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;

	unique_ptr<TableRef> GetTableRef() override;
};

} // namespace sabot_sql
