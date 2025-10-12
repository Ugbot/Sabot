//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/function_entry.hpp"
#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/function/function.hpp"
#include "sabot_sql/parser/parsed_data/create_aggregate_function_info.hpp"
#include "sabot_sql/main/attached_database.hpp"

namespace sabot_sql {

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::AGGREGATE_FUNCTION_ENTRY;
	static constexpr const char *Name = "aggregate function";

public:
	AggregateFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateAggregateFunctionInfo &info)
	    : FunctionEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, catalog, schema, info), functions(info.functions) {
		for (auto &function : functions.functions) {
			function.catalog_name = catalog.GetAttached().GetName();
			function.schema_name = schema.name;
		}
	}

	//! The aggregate functions
	AggregateFunctionSet functions;
};
} // namespace sabot_sql
