//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_vacuum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/vacuum_info.hpp"

namespace sabot_sql {

//! PhysicalVacuum represents a VACUUM operation (i.e. VACUUM or ANALYZE)
class PhysicalVacuum : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::VACUUM;

public:
	PhysicalVacuum(PhysicalPlan &physical_plan, unique_ptr<VacuumInfo> info, optional_ptr<TableCatalogEntry> table,
	               unordered_map<idx_t, idx_t> column_id_map, idx_t estimated_cardinality);

	unique_ptr<VacuumInfo> info;
	optional_ptr<TableCatalogEntry> table;
	unordered_map<idx_t, idx_t> column_id_map;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return info->has_table;
	}

	bool ParallelSink() const override {
		return IsSink();
	}
};

} // namespace sabot_sql
