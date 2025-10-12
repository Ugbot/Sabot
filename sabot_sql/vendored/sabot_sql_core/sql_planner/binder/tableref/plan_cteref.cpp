#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/operator/logical_cteref.hpp"
#include "sabot_sql/planner/tableref/bound_cteref.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTERef &ref) {
	return make_uniq<LogicalCTERef>(ref.bind_index, ref.cte_index, ref.types, ref.bound_columns, ref.is_recurring);
}

} // namespace sabot_sql
