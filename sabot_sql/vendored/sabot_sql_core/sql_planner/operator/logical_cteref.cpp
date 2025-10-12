#include "sabot_sql/planner/operator/logical_cteref.hpp"

#include "sabot_sql/main/config.hpp"

namespace sabot_sql {

InsertionOrderPreservingMap<string> LogicalCTERef::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Index"] = StringUtil::Format("%llu", cte_index);
	SetParamsEstimatedCardinality(result);
	return result;
}

vector<idx_t> LogicalCTERef::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalCTERef::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace sabot_sql
