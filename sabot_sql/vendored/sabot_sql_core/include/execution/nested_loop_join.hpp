//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/nested_loop_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types/column/column_data_collection.hpp"
#include "sabot_sql/common/types/vector.hpp"
#include "sabot_sql/planner/operator/logical_comparison_join.hpp"

namespace sabot_sql {
class ColumnDataCollection;

struct NestedLoopJoinInner {
	static idx_t Perform(idx_t &ltuple, idx_t &rtuple, DataChunk &left_conditions, DataChunk &right_conditions,
	                     SelectionVector &lvector, SelectionVector &rvector, const vector<JoinCondition> &conditions);
};

struct NestedLoopJoinMark {
	static void Perform(DataChunk &left, ColumnDataCollection &right, bool found_match[],
	                    const vector<JoinCondition> &conditions);
};

} // namespace sabot_sql
