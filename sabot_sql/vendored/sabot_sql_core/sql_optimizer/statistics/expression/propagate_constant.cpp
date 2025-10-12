#include "sabot_sql/optimizer/statistics_propagator.hpp"
#include "sabot_sql/planner/expression/bound_constant_expression.hpp"
#include "sabot_sql/storage/statistics/distinct_statistics.hpp"
#include "sabot_sql/storage/statistics/list_stats.hpp"
#include "sabot_sql/storage/statistics/struct_stats.hpp"

namespace sabot_sql {

unique_ptr<BaseStatistics> StatisticsPropagator::StatisticsFromValue(const Value &input) {
	return BaseStatistics::FromConstant(input).ToUnique();
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundConstantExpression &constant,
                                                                     unique_ptr<Expression> &expr_ptr) {
	return StatisticsFromValue(constant.value);
}

} // namespace sabot_sql
