//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/table_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/function_entry.hpp"
#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/function/function.hpp"
#include "sabot_sql/parser/parsed_data/create_table_function_info.hpp"

namespace sabot_sql {

//! A table function in the catalog
class TableFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TABLE_FUNCTION_ENTRY;
	static constexpr const char *Name = "table function";

public:
	TableFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableFunctionInfo &info);

	//! The table function
	TableFunctionSet functions;

public:
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
};
} // namespace sabot_sql
