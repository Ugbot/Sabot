//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/query_node/bound_set_operation_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/set_operation_type.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/bound_query_node.hpp"

namespace sabot_sql {

struct BoundSetOpChild {
	unique_ptr<BoundQueryNode> node;
	shared_ptr<Binder> binder;
	//! Exprs used by the UNION BY NAME operations to add a new projection
	vector<unique_ptr<Expression>> reorder_expressions;
};

//! Bound equivalent of SetOperationNode
class BoundSetOperationNode : public BoundQueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::SET_OPERATION_NODE;

public:
	BoundSetOperationNode() : BoundQueryNode(QueryNodeType::SET_OPERATION_NODE) {
	}

	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! whether the ALL modifier was used or not
	bool setop_all = false;
	//! The bound children
	vector<BoundSetOpChild> bound_children;

	//! Index used by the set operation
	idx_t setop_index;

public:
	idx_t GetRootIndex() override {
		return setop_index;
	}
};

} // namespace sabot_sql
