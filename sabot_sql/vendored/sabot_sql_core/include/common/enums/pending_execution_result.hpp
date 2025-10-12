//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/pending_execution_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class PendingExecutionResult : uint8_t {
	RESULT_READY,
	RESULT_NOT_READY,
	EXECUTION_ERROR,
	BLOCKED,
	NO_TASKS_AVAILABLE,
	EXECUTION_FINISHED
};

} // namespace sabot_sql
