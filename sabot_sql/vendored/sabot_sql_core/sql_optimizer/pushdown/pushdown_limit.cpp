#include "sabot_sql/optimizer/filter_pushdown.hpp"
#include "sabot_sql/optimizer/optimizer.hpp"
#include "sabot_sql/planner/expression/bound_columnref_expression.hpp"
#include "sabot_sql/planner/operator/logical_empty_result.hpp"
#include "sabot_sql/planner/operator/logical_limit.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> FilterPushdown::PushdownLimit(unique_ptr<LogicalOperator> op) {
	auto &limit = op->Cast<LogicalLimit>();

	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.limit_val.GetConstantValue() == 0) {
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	return FinishPushdown(std::move(op));
}

} // namespace sabot_sql
