#include "sabot_sql/optimizer/filter_pushdown.hpp"
#include "sabot_sql/planner/operator/logical_empty_result.hpp"
#include "sabot_sql/planner/operator/logical_filter.hpp"

namespace sabot_sql {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	if (filter.HasProjectionMap()) {
		return FinishPushdown(std::move(op));
	}
	// filter: gather the filters and remove the filter from the set of operations
	for (auto &expression : filter.expressions) {
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}
	GenerateFilters();
	return Rewrite(std::move(filter.children[0]));
}

} // namespace sabot_sql
