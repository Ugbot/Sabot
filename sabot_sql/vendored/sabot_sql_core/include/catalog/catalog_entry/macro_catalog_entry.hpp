//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/macro_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/catalog/catalog_entry/function_entry.hpp"
#include "sabot_sql/function/macro_function.hpp"
#include "sabot_sql/parser/parsed_data/create_macro_info.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

//! A macro function in the catalog
class MacroCatalogEntry : public FunctionEntry {
public:
	MacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info);

	//! The macro function
	vector<unique_ptr<MacroFunction>> macros;

public:
	unique_ptr<CreateInfo> GetInfo() const override;

	string ToSQL() const override;
};

} // namespace sabot_sql
