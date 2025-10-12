//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/default/default_views.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_view_info.hpp"
#include "sabot_sql/catalog/default/default_generator.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

class DefaultViewGenerator : public DefaultGenerator {
public:
	DefaultViewGenerator(Catalog &catalog, SchemaCatalogEntry &schema);

	SchemaCatalogEntry &schema;

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace sabot_sql
