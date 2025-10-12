//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_explain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/statement/explain_statement.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

class LogicalExplain : public LogicalOperator {
	explicit LogicalExplain(ExplainType explain_type)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN), explain_type(explain_type) {};

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXPLAIN;

public:
	LogicalExplain(unique_ptr<LogicalOperator> plan, ExplainType explain_type, ExplainFormat explain_format)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN), explain_type(explain_type),
	      explain_format(explain_format) {
		children.push_back(std::move(plan));
	}

	ExplainType explain_type;
	ExplainFormat explain_format;
	string physical_plan;
	string logical_plan_unopt;
	string logical_plan_opt;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override {
		return 3;
	}
	//! Skips the serialization check in VerifyPlan
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(0, 0), ColumnBinding(0, 1)};
	}
};
} // namespace sabot_sql
