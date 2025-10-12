#include "sabot_sql/planner/operator/logical_unnest.hpp"

#include "sabot_sql/main/config.hpp"

namespace sabot_sql {

vector<ColumnBinding> LogicalUnnest::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t i = 0; i < expressions.size(); i++) {
		child_bindings.emplace_back(unnest_index, i);
	}
	return child_bindings;
}

void LogicalUnnest::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalUnnest::GetTableIndex() const {
	return vector<idx_t> {unnest_index};
}

string LogicalUnnest::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", unnest_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace sabot_sql
