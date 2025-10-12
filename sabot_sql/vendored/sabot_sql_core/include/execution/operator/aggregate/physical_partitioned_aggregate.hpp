//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/aggregate/physical_partitioned_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "sabot_sql/parser/group_by_node.hpp"
#include "sabot_sql/execution/radix_partitioned_hashtable.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {

//! PhysicalPartitionedAggregate is an aggregate operator that can only perform aggregates on data that is partitioned
// by the grouping columns
class PhysicalPartitionedAggregate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PARTITIONED_AGGREGATE;

public:
	PhysicalPartitionedAggregate(PhysicalPlan &physical_plan, ClientContext &context, vector<LogicalType> types,
	                             vector<unique_ptr<Expression>> expressions, vector<unique_ptr<Expression>> groups,
	                             vector<column_t> partitions, idx_t estimated_cardinality);

	//! The partitions over which this is grouped
	vector<column_t> partitions;
	//! The groups over which the aggregate is partitioned - note that this is only
	vector<unique_ptr<Expression>> groups;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	OperatorPartitionInfo RequiredPartitionInfo() const override;
	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

} // namespace sabot_sql
