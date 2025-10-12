//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/aggregate/physical_ungrouped_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "sabot_sql/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "sabot_sql/parser/group_by_node.hpp"
#include "sabot_sql/execution/radix_partitioned_hashtable.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {

//! PhysicalUngroupedAggregate is an aggregate operator that can only perform aggregates without any groups
class PhysicalUngroupedAggregate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNGROUPED_AGGREGATE;

public:
	PhysicalUngroupedAggregate(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                           vector<unique_ptr<Expression>> expressions, idx_t estimated_cardinality,
	                           TupleDataValidityType distinct_validity);

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	unique_ptr<DistinctAggregateData> distinct_data;
	unique_ptr<DistinctAggregateCollectionInfo> distinct_collection_info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	bool SinkOrderDependent() const override;

private:
	//! Finalize the distinct aggregates
	SinkFinalizeType FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  GlobalSinkState &gstate) const;
	//! Combine the distinct aggregates
	void CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const;
	//! Sink the distinct aggregates
	void SinkDistinct(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
};

} // namespace sabot_sql
