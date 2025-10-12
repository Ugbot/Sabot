#include "sabot_sql/planner/operator/logical_any_join.hpp"

namespace sabot_sql {

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::LOGICAL_ANY_JOIN) {
}

InsertionOrderPreservingMap<string> LogicalAnyJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Condition"] = condition->ToString();
	SetParamsEstimatedCardinality(result);
	return result;
}

} // namespace sabot_sql
