//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/standard_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry.hpp"
#include "sabot_sql/catalog/dependency_list.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

//! A StandardEntry is a catalog entry that is a member of a schema
class StandardEntry : public InCatalogEntry {
public:
	StandardEntry(CatalogType type, SchemaCatalogEntry &schema, Catalog &catalog, string name)
	    : InCatalogEntry(type, catalog, std::move(name)), schema(schema) {
	}
	~StandardEntry() override {
	}

	//! The schema the entry belongs to
	SchemaCatalogEntry &schema;
	//! The dependencies of the entry, can be empty
	LogicalDependencyList dependencies;

public:
	SchemaCatalogEntry &ParentSchema() override {
		return schema;
	}
	const SchemaCatalogEntry &ParentSchema() const override {
		return schema;
	}
};

} // namespace sabot_sql
