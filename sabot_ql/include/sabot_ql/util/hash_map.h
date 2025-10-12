#pragma once

// High-performance hash map from QLever
// Using absl::flat_hash_map for fast lookups

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace sabot_ql {

// Wrapper for hash maps (from QLever)
template <typename... Ts>
using HashMap = absl::flat_hash_map<Ts...>;

// Wrapper for hash sets (from QLever)
template <typename... Ts>
using HashSet = absl::flat_hash_set<Ts...>;

} // namespace sabot_ql
