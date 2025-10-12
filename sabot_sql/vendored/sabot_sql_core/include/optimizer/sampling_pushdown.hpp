//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/sampling_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/common/unique_ptr.hpp"

namespace sabot_sql {
class LocigalOperator;
class Optimizer;

class SamplingPushdown {
public:
	//! Optimize SYSTEM SAMPLING + SCAN to SAMPLE SCAN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
};

} // namespace sabot_sql
