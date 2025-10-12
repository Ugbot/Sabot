//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/on_entry_not_found.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class OnEntryNotFound : uint8_t { THROW_EXCEPTION = 0, RETURN_NULL = 1 };

} // namespace sabot_sql
