#include "sabot_sql/optimizer/statistics_propagator.hpp"
#include "sabot_sql/planner/expression/bound_columnref_expression.hpp"

namespace sabot_sql {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundColumnRefExpression &colref,
                                                                     unique_ptr<Expression> &expr_ptr) {
	auto stats = statistics_map.find(colref.binding);
	if (stats == statistics_map.end()) {
		return nullptr;
	}
	return stats->second->ToUnique();
}

} // namespace sabot_sql
