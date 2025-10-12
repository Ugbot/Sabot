#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/tableref/bound_subqueryref.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSubqueryRef &ref) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	ref.binder->is_outside_flattened = is_outside_flattened;
	auto subquery = ref.binder->CreatePlan(*ref.subquery);
	if (ref.binder->has_unplanned_dependent_joins) {
		has_unplanned_dependent_joins = true;
	}
	return subquery;
}

} // namespace sabot_sql
