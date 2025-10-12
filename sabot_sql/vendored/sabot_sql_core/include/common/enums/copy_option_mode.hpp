//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/copy_option_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {

enum class CopyOptionMode { WRITE_ONLY, READ_ONLY, READ_WRITE };

} // namespace sabot_sql
