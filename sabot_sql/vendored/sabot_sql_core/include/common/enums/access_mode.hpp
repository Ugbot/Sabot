//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/access_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class AccessMode : uint8_t { UNDEFINED = 0, AUTOMATIC = 1, READ_ONLY = 2, READ_WRITE = 3 };

} // namespace sabot_sql
