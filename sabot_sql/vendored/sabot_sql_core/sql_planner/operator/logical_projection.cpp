#include "sabot_sql/planner/operator/logical_projection.hpp"

#include "sabot_sql/main/config.hpp"

namespace sabot_sql {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)), table_index(table_index) {
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalProjection::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalProjection::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace sabot_sql
