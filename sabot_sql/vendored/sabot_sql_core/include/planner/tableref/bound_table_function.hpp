//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/tableref/bound_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/bound_tableref.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/planner/tableref/bound_subqueryref.hpp"

namespace sabot_sql {

//! Represents a reference to a table-producing function call
class BoundTableFunction : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::TABLE_FUNCTION;

public:
	explicit BoundTableFunction(unique_ptr<LogicalOperator> get)
	    : BoundTableRef(TableReferenceType::TABLE_FUNCTION), get(std::move(get)) {
	}

	unique_ptr<LogicalOperator> get;
	unique_ptr<BoundSubqueryRef> subquery;
};

} // namespace sabot_sql
