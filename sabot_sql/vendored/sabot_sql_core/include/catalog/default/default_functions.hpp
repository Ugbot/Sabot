//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/default/default_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/default/default_generator.hpp"
#include "sabot_sql/parser/parsed_data/create_macro_info.hpp"
#include "sabot_sql/common/array_ptr.hpp"
#include "sabot_sql/catalog/default/default_table_functions.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

struct DefaultMacro {
	const char *schema;
	const char *name;
	const char *parameters[8];
	DefaultNamedParameter named_parameters[8];
	const char *macro;
};

class DefaultFunctionGenerator : public DefaultGenerator {
public:
	DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

	SABOT_SQL_API static unique_ptr<CreateMacroInfo> CreateInternalMacroInfo(const DefaultMacro &default_macro);
	SABOT_SQL_API static unique_ptr<CreateMacroInfo> CreateInternalMacroInfo(array_ptr<const DefaultMacro> macro);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace sabot_sql
