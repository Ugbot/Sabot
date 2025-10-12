//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/main/prepared_statement_data.hpp"

namespace sabot_sql {

class PhysicalExecute : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXECUTE;

public:
	PhysicalExecute(PhysicalPlan &physical_plan, PhysicalOperator &plan);

	PhysicalOperator &plan;
	shared_ptr<PreparedStatementData> prepared;

public:
	vector<const_reference<PhysicalOperator>> GetChildren() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace sabot_sql
