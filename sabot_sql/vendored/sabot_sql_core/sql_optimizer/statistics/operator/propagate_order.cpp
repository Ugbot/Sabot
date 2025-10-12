
#include "sabot_sql/optimizer/statistics_propagator.hpp"
#include "sabot_sql/planner/operator/logical_order.hpp"

namespace sabot_sql {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOrder &order,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(order.children[0]);

	// then propagate to each of the order expressions
	for (auto &bound_order : order.orders) {
		bound_order.stats = PropagateExpression(bound_order.expression);
	}
	return std::move(node_stats);
}

} // namespace sabot_sql
