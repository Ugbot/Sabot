//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/projection/physical_pivot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/common/string_map_set.hpp"
#include "sabot_sql/planner/tableref/bound_pivotref.hpp"

namespace sabot_sql {

//! PhysicalPivot implements the physical PIVOT operation
class PhysicalPivot : public PhysicalOperator {
public:
	PhysicalPivot(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperator &child,
	              BoundPivotInfo bound_pivot);

	BoundPivotInfo bound_pivot;
	//! The map for pivot value -> column index
	string_map_t<idx_t> pivot_map;
	//! The empty aggregate values
	vector<Value> empty_aggregates;

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}
};

} // namespace sabot_sql
