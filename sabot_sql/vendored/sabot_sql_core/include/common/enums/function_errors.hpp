//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/function_errors.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {

//! Whether or not a function can throw an error or not
enum class FunctionErrors : uint8_t { CANNOT_ERROR = 0, CAN_THROW_RUNTIME_ERROR = 1 };

} // namespace sabot_sql
