//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/arrow/arrow_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/common/arrow/arrow.hpp"
#include "sabot_sql/main/client_properties.hpp"
#include <list>

namespace sabot_sql {
class ArrowTypeExtensionData;
struct DBConfig;
struct ArrowConverter {
	SABOT_SQL_API static void ToArrowSchema(ArrowSchema *out_schema, const vector<LogicalType> &types,
	                                     const vector<string> &names, ClientProperties &options);
	SABOT_SQL_API static void
	ToArrowArray(DataChunk &input, ArrowArray *out_array, ClientProperties options,
	             const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &extension_type_cast);
};

//===--------------------------------------------------------------------===//
// Arrow Schema
//===--------------------------------------------------------------------===//
struct SabotSQLArrowSchemaHolder {
	// unused in children
	vector<ArrowSchema> children;
	// unused in children
	vector<ArrowSchema *> children_ptrs;
	//! used for nested structures
	std::list<vector<ArrowSchema>> nested_children;
	std::list<vector<ArrowSchema *>> nested_children_ptr;
	//! This holds strings created to represent decimal types
	vector<unsafe_unique_array<char>> owned_type_names;
	vector<unsafe_unique_array<char>> owned_column_names;
	//! This holds any values created for metadata info
	vector<unsafe_unique_array<char>> metadata_info;
	vector<unsafe_unique_array<char>> extension_format;
};

} // namespace sabot_sql
