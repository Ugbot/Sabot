//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/catalog_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class CatalogType : uint8_t {
	INVALID = 0,
	TABLE_ENTRY = 1,
	SCHEMA_ENTRY = 2,
	VIEW_ENTRY = 3,
	INDEX_ENTRY = 4,
	PREPARED_STATEMENT = 5,
	SEQUENCE_ENTRY = 6,
	COLLATION_ENTRY = 7,
	TYPE_ENTRY = 8,
	DATABASE_ENTRY = 9,

	// functions
	TABLE_FUNCTION_ENTRY = 25,
	SCALAR_FUNCTION_ENTRY = 26,
	AGGREGATE_FUNCTION_ENTRY = 27,
	PRAGMA_FUNCTION_ENTRY = 28,
	COPY_FUNCTION_ENTRY = 29,
	MACRO_ENTRY = 30,
	TABLE_MACRO_ENTRY = 31,

	// version info
	DELETED_ENTRY = 51,
	RENAMED_ENTRY = 52,

	// secrets
	SECRET_ENTRY = 71,
	SECRET_TYPE_ENTRY = 72,
	SECRET_FUNCTION_ENTRY = 73,

	// dependency info
	DEPENDENCY_ENTRY = 100

};

SABOT_SQL_API string CatalogTypeToString(CatalogType type);
CatalogType CatalogTypeFromString(const string &type);

} // namespace sabot_sql
