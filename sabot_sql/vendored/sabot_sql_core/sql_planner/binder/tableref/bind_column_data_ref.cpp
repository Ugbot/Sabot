#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/parser/tableref/column_data_ref.hpp"
#include "sabot_sql/planner/tableref/bound_column_data_ref.hpp"
#include "sabot_sql/planner/operator/logical_column_data_get.hpp"

namespace sabot_sql {

unique_ptr<BoundTableRef> Binder::Bind(ColumnDataRef &ref) {
	auto &collection = *ref.collection;
	auto types = collection.Types();
	auto result = make_uniq<BoundColumnDataRef>(std::move(ref.collection));
	result->bind_index = GenerateTableIndex();
	for (idx_t i = ref.expected_names.size(); i < types.size(); i++) {
		ref.expected_names.push_back("col" + to_string(i + 1));
	}
	bind_context.AddGenericBinding(result->bind_index, ref.alias, ref.expected_names, types);
	return unique_ptr_cast<BoundColumnDataRef, BoundTableRef>(std::move(result));
}

} // namespace sabot_sql
