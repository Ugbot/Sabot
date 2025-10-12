//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/stream_execution_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class StreamExecutionResult : uint8_t {
	CHUNK_READY,
	CHUNK_NOT_READY,
	EXECUTION_ERROR,
	EXECUTION_CANCELLED,
	BLOCKED,
	NO_TASKS_AVAILABLE,
	EXECUTION_FINISHED
};

} // namespace sabot_sql
