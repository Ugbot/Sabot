//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_buffered_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/operator/helper/physical_result_collector.hpp"
#include "sabot_sql/main/buffered_data/simple_buffered_data.hpp"

namespace sabot_sql {

class PhysicalBufferedCollector : public PhysicalResultCollector {
public:
	PhysicalBufferedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data, bool parallel);

	bool parallel;

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool ParallelSink() const override;
	bool SinkOrderDependent() const override;
	bool IsStreaming() const override {
		return true;
	}
};

} // namespace sabot_sql
