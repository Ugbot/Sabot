//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/thread_pin_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class ThreadPinMode : uint8_t { OFF = 0, ON = 1, AUTO = 2 };

} // namespace sabot_sql
