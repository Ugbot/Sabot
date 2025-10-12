//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/preserve_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class PreserveOrderType : uint8_t { AUTOMATIC = 0, PRESERVE_ORDER = 1, DONT_PRESERVE_ORDER = 2 };

} // namespace sabot_sql
