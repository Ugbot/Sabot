//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/persistent/physical_copy_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/operator/logical_copy_database.hpp"

namespace sabot_sql {

class PhysicalCopyDatabase : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::COPY_DATABASE;

public:
	PhysicalCopyDatabase(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                     unique_ptr<CopyDatabaseInfo> info_p);
	~PhysicalCopyDatabase() override;

	unique_ptr<CopyDatabaseInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
