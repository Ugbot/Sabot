//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/output_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class ExplainOutputType : uint8_t { ALL = 0, OPTIMIZED_ONLY = 1, PHYSICAL_ONLY = 2 };

} // namespace sabot_sql
