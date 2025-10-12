//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/tableref/bound_dummytableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/bound_tableref.hpp"

namespace sabot_sql {

//! Represents a cross product
class BoundEmptyTableRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::EMPTY_FROM;

public:
	explicit BoundEmptyTableRef(idx_t bind_index)
	    : BoundTableRef(TableReferenceType::EMPTY_FROM), bind_index(bind_index) {
	}
	idx_t bind_index;
};
} // namespace sabot_sql
