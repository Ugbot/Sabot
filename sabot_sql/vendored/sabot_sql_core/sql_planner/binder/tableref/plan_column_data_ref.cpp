#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/tableref/bound_column_data_ref.hpp"
#include "sabot_sql/planner/operator/logical_column_data_get.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundColumnDataRef &ref) {
	auto types = ref.collection->Types();
	// Create a (potentially owning) LogicalColumnDataGet
	auto root = make_uniq_base<LogicalOperator, LogicalColumnDataGet>(ref.bind_index, std::move(types),
	                                                                  std::move(ref.collection));
	return root;
}

} // namespace sabot_sql
