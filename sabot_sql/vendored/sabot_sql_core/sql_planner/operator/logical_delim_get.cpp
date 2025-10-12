#include "sabot_sql/planner/operator/logical_delim_get.hpp"

#include "sabot_sql/main/config.hpp"

namespace sabot_sql {

vector<idx_t> LogicalDelimGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalDelimGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace sabot_sql
