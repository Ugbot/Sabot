//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/parsed_data/bound_create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_table_info.hpp"
#include "sabot_sql/planner/bound_constraint.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/storage/table/persistent_table_data.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/catalog/catalog_entry/table_column_type.hpp"
#include "sabot_sql/catalog/catalog_entry/column_dependency_manager.hpp"
#include "sabot_sql/catalog/dependency_list.hpp"
#include "sabot_sql/storage/table_storage_info.hpp"

namespace sabot_sql {
class CatalogEntry;

struct BoundCreateTableInfo {
	explicit BoundCreateTableInfo(SchemaCatalogEntry &schema, unique_ptr<CreateInfo> base_p)
	    : schema(schema), base(std::move(base_p)) {
		D_ASSERT(base);
	}

	//! The schema to create the table in
	SchemaCatalogEntry &schema;
	//! The base CreateInfo object
	unique_ptr<CreateInfo> base;
	//! Column dependency manager of the table
	ColumnDependencyManager column_dependency_manager;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! Dependents of the table (in e.g. default values)
	LogicalDependencyList dependencies;
	//! The existing table data on disk (if any)
	unique_ptr<PersistentTableData> data;
	//! CREATE TABLE from QUERY
	unique_ptr<LogicalOperator> query;
	//! Indexes created by this table
	vector<IndexStorageInfo> indexes;

	CreateTableInfo &Base() {
		D_ASSERT(base);
		return base->Cast<CreateTableInfo>();
	}
};

} // namespace sabot_sql
