//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/table_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {

struct TableColumn {
	TableColumn() = default;
	TableColumn(string name_p, LogicalType type_p) : name(std::move(name_p)), type(std::move(type_p)) {
	}

	string name;
	LogicalType type;

	void Serialize(Serializer &serializer) const;
	static TableColumn Deserialize(Deserializer &deserializer);
};

using virtual_column_map_t = unordered_map<column_t, TableColumn>;

} // namespace sabot_sql
