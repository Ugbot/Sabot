//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/type_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/standard_entry.hpp"
#include "sabot_sql/common/mutex.hpp"
#include "sabot_sql/parser/parsed_data/create_type_info.hpp"
#include "sabot_sql/catalog/dependency_list.hpp"

namespace sabot_sql {

//! A type catalog entry
class TypeCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TYPE_ENTRY;
	static constexpr const char *Name = "type";

public:
	//! Create a TypeCatalogEntry and initialize storage for it
	TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info);

	LogicalType user_type;

	bind_logical_type_function_t bind_function;

public:
	unique_ptr<CreateInfo> GetInfo() const override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	string ToSQL() const override;
};
} // namespace sabot_sql
