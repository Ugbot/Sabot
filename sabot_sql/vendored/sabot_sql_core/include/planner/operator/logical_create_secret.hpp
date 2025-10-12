//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// planner/operator/logical_create_secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_secret_info.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

//! LogicalCreateSecret represents a simple logical operator that only passes on the parse info
class LogicalCreateSecret : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_SECRET;

public:
	explicit LogicalCreateSecret(CreateSecretInput secret_input_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_SECRET), secret_input(std::move(secret_input_p)) {
	}

	CreateSecretInput secret_input;

public:
	idx_t EstimateCardinality(ClientContext &context) override {
		return 1;
	};

	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};
} // namespace sabot_sql
