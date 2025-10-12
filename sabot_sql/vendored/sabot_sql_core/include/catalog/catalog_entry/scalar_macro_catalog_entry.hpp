//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/catalog/standard_entry.hpp"
#include "sabot_sql/parser/parsed_data/create_macro_info.hpp"
#include "sabot_sql/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace sabot_sql {

//! A macro function in the catalog
class ScalarMacroCatalogEntry : public MacroCatalogEntry {
public:
	static constexpr const CatalogType Type = CatalogType::MACRO_ENTRY;
	static constexpr const char *Name = "macro function";

public:
	ScalarMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
};
} // namespace sabot_sql
