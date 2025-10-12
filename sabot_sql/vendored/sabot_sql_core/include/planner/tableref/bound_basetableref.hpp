//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/tableref/bound_basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/bound_tableref.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {
class TableCatalogEntry;

//! Represents a TableReference to a base table in the schema
class BoundBaseTableRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

public:
	BoundBaseTableRef(TableCatalogEntry &table, unique_ptr<LogicalOperator> get)
	    : BoundTableRef(TableReferenceType::BASE_TABLE), table(table), get(std::move(get)) {
	}

	TableCatalogEntry &table;
	unique_ptr<LogicalOperator> get;
};
} // namespace sabot_sql
