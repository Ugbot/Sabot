//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/debug_initialize.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class DebugInitialize : uint8_t { NO_INITIALIZE = 0, DEBUG_ZERO_INITIALIZE = 1, DEBUG_ONE_INITIALIZE = 2 };

} // namespace sabot_sql
