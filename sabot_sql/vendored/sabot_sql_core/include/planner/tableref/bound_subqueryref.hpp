//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/tableref/bound_subqueryref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/bound_query_node.hpp"
#include "sabot_sql/planner/bound_tableref.hpp"

namespace sabot_sql {

//! Represents a cross product
class BoundSubqueryRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::SUBQUERY;

public:
	BoundSubqueryRef(shared_ptr<Binder> binder_p, unique_ptr<BoundQueryNode> subquery)
	    : BoundTableRef(TableReferenceType::SUBQUERY), binder(std::move(binder_p)), subquery(std::move(subquery)) {
	}

	//! The binder used to bind the subquery
	shared_ptr<Binder> binder;
	//! The bound subquery node (if any)
	unique_ptr<BoundQueryNode> subquery;
};
} // namespace sabot_sql
