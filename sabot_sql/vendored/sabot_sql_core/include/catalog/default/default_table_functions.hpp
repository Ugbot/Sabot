//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/default/default_table_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/default/default_generator.hpp"
#include "sabot_sql/parser/parsed_data/create_macro_info.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

struct DefaultNamedParameter {
	const char *name;
	const char *default_value;
};

struct DefaultTableMacro {
	const char *schema;
	const char *name;
	const char *parameters[8];
	DefaultNamedParameter named_parameters[8];
	const char *macro;
};

class DefaultTableFunctionGenerator : public DefaultGenerator {
public:
	DefaultTableFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;

	static unique_ptr<CreateMacroInfo> CreateTableMacroInfo(const DefaultTableMacro &default_macro);

private:
	static unique_ptr<CreateMacroInfo> CreateInternalTableMacroInfo(const DefaultTableMacro &default_macro,
	                                                                unique_ptr<MacroFunction> function);
};

} // namespace sabot_sql
