#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/operator/logical_get.hpp"
#include "sabot_sql/planner/tableref/bound_basetableref.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundBaseTableRef &ref) {
	return std::move(ref.get);
}

} // namespace sabot_sql
