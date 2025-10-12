//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/copy_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/standard_entry.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/function/copy_function.hpp"

namespace sabot_sql {

class Catalog;
struct CreateCopyFunctionInfo;

//! A table function in the catalog
class CopyFunctionCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::COPY_FUNCTION_ENTRY;
	static constexpr const char *Name = "copy function";

public:
	CopyFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateCopyFunctionInfo &info);

	//! The copy function
	CopyFunction function;
};
} // namespace sabot_sql
