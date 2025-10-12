#include "sabot_sql/optimizer/filter_pushdown.hpp"
#include "sabot_sql/planner/expression_iterator.hpp"
#include "sabot_sql/planner/operator/logical_distinct.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> FilterPushdown::PushdownDistinct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_DISTINCT);
	auto &distinct = op->Cast<LogicalDistinct>();
	if (!distinct.order_by) {
		// regular DISTINCT - can just push down
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	// no pushdown through DISTINCT ON (yet?)
	return FinishPushdown(std::move(op));
}

} // namespace sabot_sql
