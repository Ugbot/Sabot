//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/storage_block_prefetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class StorageBlockPrefetch : uint8_t { REMOTE_ONLY = 0, NEVER = 1, ALWAYS_PREFETCH = 2, DEBUG_FORCE_ALWAYS = 3 };

} // namespace sabot_sql
