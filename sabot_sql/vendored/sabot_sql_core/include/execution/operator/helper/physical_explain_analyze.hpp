//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_explain_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/common/enums/explain_format.hpp"

namespace sabot_sql {

class PhysicalExplainAnalyze : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXPLAIN_ANALYZE;

public:
	PhysicalExplainAnalyze(PhysicalPlan &physical_plan, vector<LogicalType> types, ExplainFormat format)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXPLAIN_ANALYZE, std::move(types), 1), format(format) {
	}

public:
	ExplainFormat format;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink Interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

} // namespace sabot_sql
