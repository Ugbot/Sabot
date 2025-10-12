#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/operator/logical_get.hpp"
#include "sabot_sql/planner/tableref/bound_basetableref.hpp"
#include "sabot_sql/planner/operator/logical_delim_get.hpp"
namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundDelimGetRef &ref) {
	return make_uniq<LogicalDelimGet>(ref.bind_index, ref.column_types);
}

} // namespace sabot_sql
