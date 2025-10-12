//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/aggregate_handling.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

//===----
enum class AggregateHandling : uint8_t {
	STANDARD_HANDLING,     // standard handling as in the SELECT clause
	NO_AGGREGATES_ALLOWED, // no aggregates allowed: any aggregates in this node will result in an error
	FORCE_AGGREGATES       // force aggregates: any non-aggregate select list entry will become a GROUP
};

const char *ToString(AggregateHandling value);

} // namespace sabot_sql
