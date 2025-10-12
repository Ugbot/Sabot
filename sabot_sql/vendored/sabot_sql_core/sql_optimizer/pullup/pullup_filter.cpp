#include "sabot_sql/optimizer/filter_pullup.hpp"
#include "sabot_sql/planner/expression/bound_between_expression.hpp"
#include "sabot_sql/planner/expression/bound_comparison_expression.hpp"
#include "sabot_sql/planner/expression_iterator.hpp"
#include "sabot_sql/planner/operator/logical_filter.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> FilterPullup::PullupFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);

	auto &filter = op->Cast<LogicalFilter>();
	if (can_pullup && !filter.HasProjectionMap()) {
		unique_ptr<LogicalOperator> child = std::move(op->children[0]);
		child = Rewrite(std::move(child));
		// moving filter's expressions
		for (idx_t i = 0; i < op->expressions.size(); ++i) {
			filters_expr_pullup.push_back(std::move(op->expressions[i]));
		}
		return child;
	}
	op->children[0] = Rewrite(std::move(op->children[0]));
	return op;
}

} // namespace sabot_sql
