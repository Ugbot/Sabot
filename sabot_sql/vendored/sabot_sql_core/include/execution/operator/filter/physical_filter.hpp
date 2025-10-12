//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/filter/physical_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

//! PhysicalFilter represents a filter operator. It removes non-matching tuples
//! from the result. Note that it does not physically change the data, it only
//! adds a selection vector to the chunk.
class PhysicalFilter : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FILTER;

public:
	PhysicalFilter(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	               idx_t estimated_cardinality);

	//! The filter expression
	unique_ptr<Expression> expression;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};
} // namespace sabot_sql
