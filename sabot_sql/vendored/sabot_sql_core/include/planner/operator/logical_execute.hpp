//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/prepared_statement_data.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

class LogicalExecute : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXECUTE;

public:
	explicit LogicalExecute(shared_ptr<PreparedStatementData> prepared_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXECUTE), prepared(std::move(prepared_p)) {
		D_ASSERT(prepared);
		types = prepared->types;
	}

	shared_ptr<PreparedStatementData> prepared;

public:
	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types = prepared->types;
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(0, types.size());
	}
};
} // namespace sabot_sql
