//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/common/enums/physical_operator_type.hpp"
#include "sabot_sql/main/prepared_statement_data.hpp"

namespace sabot_sql {

class PhysicalPrepare : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PREPARE;

public:
	PhysicalPrepare(PhysicalPlan &physical_plan, string name_p, shared_ptr<PreparedStatementData> prepared,
	                idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::PREPARE, {LogicalType::BOOLEAN}, estimated_cardinality),
	      name(std::move(name_p)), prepared(std::move(prepared)) {
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
