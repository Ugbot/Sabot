#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/operator/logical_dummy_scan.hpp"
#include "sabot_sql/planner/tableref/bound_dummytableref.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundEmptyTableRef &ref) {
	return make_uniq<LogicalDummyScan>(ref.bind_index);
}

} // namespace sabot_sql
