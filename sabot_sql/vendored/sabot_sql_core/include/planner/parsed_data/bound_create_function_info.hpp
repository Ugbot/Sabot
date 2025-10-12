//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/parsed_data/bound_create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_macro_info.hpp"

namespace sabot_sql {
class CatalogEntry;

struct BoundCreateFunctionInfo {
	explicit BoundCreateFunctionInfo(SchemaCatalogEntry &schema, unique_ptr<CreateInfo> base)
	    : schema(schema), base(std::move(base)) {
	}

	//! The schema to create the table in
	SchemaCatalogEntry &schema;
	//! The base CreateInfo object
	unique_ptr<CreateInfo> base;

	CreateMacroInfo &Base() {
		return (CreateMacroInfo &)*base;
	}
};

} // namespace sabot_sql
