//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/schema/physical_create_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/create_view_info.hpp"

namespace sabot_sql {

//! PhysicalCreateView represents a CREATE VIEW command
class PhysicalCreateView : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_VIEW;

public:
	explicit PhysicalCreateView(PhysicalPlan &physical_plan, unique_ptr<CreateViewInfo> info,
	                            idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_VIEW, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<CreateViewInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
