//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/default/default_schemas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/default/default_generator.hpp"

namespace sabot_sql {

class DefaultSchemaGenerator : public DefaultGenerator {
public:
	explicit DefaultSchemaGenerator(Catalog &catalog);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(CatalogTransaction transaction, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
	static bool IsDefaultSchema(const string &input_schema);
};

} // namespace sabot_sql
