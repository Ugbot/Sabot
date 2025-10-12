//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/main/prepared_statement_data.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

class TableCatalogEntry;

class LogicalPrepare : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PREPARE;

public:
	LogicalPrepare(string name_p, shared_ptr<PreparedStatementData> prepared, unique_ptr<LogicalOperator> logical_plan)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_PREPARE), name(std::move(name_p)),
	      prepared(std::move(prepared)) {
		if (logical_plan) {
			children.push_back(std::move(logical_plan));
		}
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

public:
	idx_t EstimateCardinality(ClientContext &context) override;
	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}

	bool RequireOptimizer() const override {
		if (!prepared->properties.bound_all_parameters || prepared->properties.always_require_rebind) {
			return false;
		}
		return children[0]->RequireOptimizer();
	}
};
} // namespace sabot_sql
