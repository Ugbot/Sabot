//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/checkpoint_abort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {

enum class CheckpointAbort : uint8_t {
	NO_ABORT = 0,
	DEBUG_ABORT_BEFORE_TRUNCATE = 1,
	DEBUG_ABORT_BEFORE_HEADER = 2,
	DEBUG_ABORT_AFTER_FREE_LIST_WRITE = 3
};

} // namespace sabot_sql
