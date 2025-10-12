//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/sel_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/vector_buffer.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {

//! Selection vector cache used for caching vector slices
struct SelCache {
	unordered_map<sel_t *, buffer_ptr<VectorBuffer>> cache;
};

} // namespace sabot_sql
