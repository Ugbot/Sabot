//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/default/default_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types.hpp"
#include "sabot_sql/catalog/default/default_generator.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

class DefaultTypeGenerator : public DefaultGenerator {
public:
	DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	SABOT_SQL_API static LogicalTypeId GetDefaultType(const string &name);

	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace sabot_sql
