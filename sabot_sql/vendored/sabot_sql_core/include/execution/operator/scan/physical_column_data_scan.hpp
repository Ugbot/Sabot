//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/scan/physical_column_data_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/optionally_owned_ptr.hpp"
#include "sabot_sql/common/types/column/column_data_collection.hpp"
#include "sabot_sql/execution/physical_operator.hpp"

namespace sabot_sql {

//! The PhysicalColumnDataScan scans a ColumnDataCollection
class PhysicalColumnDataScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

public:
	PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperatorType op_type,
	                       idx_t estimated_cardinality, optionally_owned_ptr<ColumnDataCollection> collection);

	PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperatorType op_type,
	                       idx_t estimated_cardinality, idx_t cte_index);

	//! (optionally owned) column data collection to scan
	optionally_owned_ptr<ColumnDataCollection> collection;

	idx_t cte_index;
	optional_idx delim_index;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	bool ParallelSource() const override {
		return true;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace sabot_sql
