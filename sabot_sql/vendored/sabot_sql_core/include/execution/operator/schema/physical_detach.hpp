//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/schema/physical_detach.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/detach_info.hpp"

namespace sabot_sql {

class PhysicalDetach : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DETACH;

public:
	PhysicalDetach(PhysicalPlan &physical_plan, unique_ptr<DetachInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::DETACH, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<DetachInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
