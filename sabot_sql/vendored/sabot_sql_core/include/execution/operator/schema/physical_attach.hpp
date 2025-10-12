//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/schema/physical_attach.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/attach_info.hpp"

namespace sabot_sql {

//! PhysicalLoad represents an extension LOAD operation
class PhysicalAttach : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ATTACH;

public:
	explicit PhysicalAttach(PhysicalPlan &physical_plan, unique_ptr<AttachInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::ATTACH, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<AttachInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
