//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/set_operation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class SetOperationType : uint8_t { NONE = 0, UNION = 1, EXCEPT = 2, INTERSECT = 3, UNION_BY_NAME = 4 };

} // namespace sabot_sql
