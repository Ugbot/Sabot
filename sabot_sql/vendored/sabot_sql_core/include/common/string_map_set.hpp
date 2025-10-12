//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/string_map_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/operator/comparison_operators.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/types/string_type.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/unordered_set.hpp"

namespace sabot_sql {

struct StringHash {
	std::size_t operator()(const string_t &k) const {
		return Hash(k);
	}
};

struct StringEquality {
	bool operator()(const string_t &a, const string_t &b) const {
		return Equals::Operation(a, b);
	}
};

template <typename T>
using string_map_t = unordered_map<string_t, T, StringHash, StringEquality>;

using string_set_t = unordered_set<string_t, StringHash, StringEquality>;

} // namespace sabot_sql
