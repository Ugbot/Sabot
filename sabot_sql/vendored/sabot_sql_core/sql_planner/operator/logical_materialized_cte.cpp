#include "sabot_sql/planner/operator/logical_materialized_cte.hpp"

namespace sabot_sql {

InsertionOrderPreservingMap<string> LogicalMaterializedCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index);
	SetParamsEstimatedCardinality(result);
	return result;
}

vector<idx_t> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace sabot_sql
