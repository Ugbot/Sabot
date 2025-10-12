//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/planner/bound_result_modifier.hpp"

namespace sabot_sql {

//! LogicalLimit represents a LIMIT clause
class LogicalLimit : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_LIMIT;

public:
	LogicalLimit(BoundLimitNode limit_val, BoundLimitNode offset_val);

	BoundLimitNode limit_val;
	BoundLimitNode offset_val;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	idx_t EstimateCardinality(ClientContext &context) override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};
} // namespace sabot_sql
