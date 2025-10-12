#include "sabot_sql/optimizer/statistics_propagator.hpp"
#include "sabot_sql/planner/operator/logical_cross_product.hpp"

namespace sabot_sql {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalCrossProduct &cp,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate statistics in the child node
	auto left_stats = PropagateStatistics(cp.children[0]);
	auto right_stats = PropagateStatistics(cp.children[1]);
	if (!left_stats || !right_stats) {
		return nullptr;
	}
	MultiplyCardinalities(left_stats, *right_stats);
	return left_stats;
}

} // namespace sabot_sql
