//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_limit_percent.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/planner/bound_result_modifier.hpp"

namespace sabot_sql {

//! PhyisicalLimitPercent represents the LIMIT PERCENT operator
class PhysicalLimitPercent : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::LIMIT_PERCENT;

public:
	PhysicalLimitPercent(PhysicalPlan &physical_plan, vector<LogicalType> types, BoundLimitNode limit_val_p,
	                     BoundLimitNode offset_val_p, idx_t estimated_cardinality);

	BoundLimitNode limit_val;
	BoundLimitNode offset_val;

public:
	bool SinkOrderDependent() const override {
		return true;
	}

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSink() const override {
		return true;
	}
};

} // namespace sabot_sql
