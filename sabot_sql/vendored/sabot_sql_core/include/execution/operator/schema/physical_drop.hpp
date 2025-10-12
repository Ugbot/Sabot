//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/schema/physical_drop.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/drop_info.hpp"

namespace sabot_sql {

//! PhysicalDrop represents a DROP [...] command
class PhysicalDrop : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DROP;

public:
	explicit PhysicalDrop(PhysicalPlan &physical_plan, unique_ptr<DropInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::DROP, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<DropInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
