//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/parser/parsed_data/create_macro_info.hpp"
#include "sabot_sql/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace sabot_sql {

//! A macro function in the catalog
class TableMacroCatalogEntry : public MacroCatalogEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TABLE_MACRO_ENTRY;
	static constexpr const char *Name = "table macro function";

public:
	TableMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
};

} // namespace sabot_sql
