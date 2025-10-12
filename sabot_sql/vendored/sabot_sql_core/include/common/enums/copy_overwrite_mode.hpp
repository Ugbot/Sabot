//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/copy_overwrite_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

enum class CopyOverwriteMode : uint8_t {
	COPY_ERROR_ON_CONFLICT = 0,
	COPY_OVERWRITE = 1,
	COPY_OVERWRITE_OR_IGNORE = 2,
	COPY_APPEND = 3
};

} // namespace sabot_sql
