//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/destroy_buffer_upon.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class DestroyBufferUpon : uint8_t {
	BLOCK = 0,    //! Destroy the data buffer upon destroying the associated BlockHandle (block can be evicted)
	EVICTION = 1, //! Destroy the data buffer upon eviction to storage (destroy instead of evict)
	UNPIN = 2     //! Destroy the data buffer upon unpin (destroyed immediately, not added to eviction queue)
};

} // namespace sabot_sql
