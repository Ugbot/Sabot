#pragma once

// Hash map wrapper using STL containers
// Note: Using std::unordered_map instead of abseil for now
// Can upgrade to abseil later for better performance if needed

#include <unordered_map>
#include <unordered_set>

namespace sabot_ql {

// Wrapper for hash maps (using STL)
template <typename... Ts>
using HashMap = std::unordered_map<Ts...>;

// Wrapper for hash sets (using STL)
template <typename... Ts>
using HashSet = std::unordered_set<Ts...>;

} // namespace sabot_ql
