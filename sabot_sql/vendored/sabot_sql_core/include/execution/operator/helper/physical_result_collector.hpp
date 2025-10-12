//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_result_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/common/enums/statement_type.hpp"

namespace sabot_sql {
class PreparedStatementData;

//! PhysicalResultCollector is an abstract class that is used to generate the final result of a query
class PhysicalResultCollector : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RESULT_COLLECTOR;

public:
	PhysicalResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data);

	StatementType statement_type;
	StatementProperties properties;
	PhysicalOperator &plan;
	vector<string> names;

public:
	static PhysicalOperator &GetResultCollector(ClientContext &context, PreparedStatementData &data);

public:
	//! The final method used to fetch the query result from this operator
	virtual unique_ptr<QueryResult> GetResult(GlobalSinkState &state) = 0;

	bool IsSink() const override {
		return true;
	}

public:
	vector<const_reference<PhysicalOperator>> GetChildren() const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	bool IsSource() const override {
		return true;
	}

public:
	//! Whether this is a streaming result collector
	virtual bool IsStreaming() const {
		return false;
	}
};

} // namespace sabot_sql
