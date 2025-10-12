//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/statistics/segment_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/storage/statistics/base_statistics.hpp"

namespace sabot_sql {

class SegmentStatistics {
public:
	explicit SegmentStatistics(LogicalType type);
	explicit SegmentStatistics(BaseStatistics statistics);

	//! Type-specific statistics of the segment
	BaseStatistics statistics;
};

} // namespace sabot_sql
