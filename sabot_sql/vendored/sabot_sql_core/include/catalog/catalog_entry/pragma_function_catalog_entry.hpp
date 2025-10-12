//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/pragma_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/function_entry.hpp"
#include "sabot_sql/function/pragma_function.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

class Catalog;
struct CreatePragmaFunctionInfo;

//! A pragma function in the catalog
class PragmaFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::PRAGMA_FUNCTION_ENTRY;
	static constexpr const char *Name = "pragma function";

public:
	PragmaFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreatePragmaFunctionInfo &info);

	//! The pragma functions
	PragmaFunctionSet functions;
};
} // namespace sabot_sql
