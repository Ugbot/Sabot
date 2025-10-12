#include "sabot_sql/optimizer/statistics_propagator.hpp"
#include "sabot_sql/planner/expression/bound_function_expression.hpp"

namespace sabot_sql {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundFunctionExpression &func,
                                                                     unique_ptr<Expression> &expr_ptr) {
	vector<BaseStatistics> stats;
	stats.reserve(func.children.size());
	for (idx_t i = 0; i < func.children.size(); i++) {
		auto stat = PropagateExpression(func.children[i]);
		if (!stat) {
			stats.push_back(BaseStatistics::CreateUnknown(func.children[i]->return_type));
		} else {
			stats.push_back(stat->Copy());
		}
	}
	if (!func.function.statistics) {
		return nullptr;
	}
	FunctionStatisticsInput input(func, func.bind_info.get(), stats, &expr_ptr);
	return func.function.statistics(context, input);
}

} // namespace sabot_sql
