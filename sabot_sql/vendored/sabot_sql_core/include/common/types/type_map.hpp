//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/type_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/unordered_set.hpp"

namespace sabot_sql {

struct LogicalTypeHashFunction {
	uint64_t operator()(const LogicalType &type) const {
		return (uint64_t)type.Hash();
	}
};

struct LogicalTypeEquality {
	bool operator()(const LogicalType &a, const LogicalType &b) const {
		return a == b;
	}
};

template <typename T>
using type_map_t = unordered_map<LogicalType, T, LogicalTypeHashFunction, LogicalTypeEquality>;

using type_set_t = unordered_set<LogicalType, LogicalTypeHashFunction, LogicalTypeEquality>;

struct LogicalTypeIdHashFunction {
	uint64_t operator()(const LogicalTypeId &type_id) const {
		return sabot_sql::Hash<uint8_t>((uint8_t)type_id);
	}
};

struct LogicalTypeIdEquality {
	bool operator()(const LogicalTypeId &a, const LogicalTypeId &b) const {
		return a == b;
	}
};

template <typename T>
using type_id_map_t = unordered_map<LogicalTypeId, T, LogicalTypeIdHashFunction, LogicalTypeIdEquality>;

using type_id_set_t = unordered_set<LogicalTypeId, LogicalTypeIdHashFunction, LogicalTypeIdEquality>;

} // namespace sabot_sql
