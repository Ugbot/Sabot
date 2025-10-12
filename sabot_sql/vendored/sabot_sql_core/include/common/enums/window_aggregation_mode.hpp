//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/window_aggregation_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class WindowAggregationMode : uint32_t {
	//! Use the window aggregate API if available
	WINDOW = 0,
	//! Don't use window, but use combine if available
	COMBINE,
	//! Don't use combine or window (compute each frame separately)
	SEPARATE
};

} // namespace sabot_sql
