//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/scan/physical_positional_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/function/table_function.hpp"
#include "sabot_sql/planner/table_filter.hpp"
#include "sabot_sql/storage/data_table.hpp"

namespace sabot_sql {

//! Represents a scan of a base table
class PhysicalPositionalScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::POSITIONAL_SCAN;

public:
	//! Regular Table Scan
	PhysicalPositionalScan(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperator &left,
	                       PhysicalOperator &right);

	//! The child table functions
	vector<reference<PhysicalOperator>> child_tables;

public:
	bool Equals(const PhysicalOperator &other) const override;
	vector<const_reference<PhysicalOperator>> GetChildren() const override;

public:
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
