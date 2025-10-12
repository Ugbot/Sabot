#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/tableref/bound_table_function.hpp"

namespace sabot_sql {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableFunction &ref) {
	if (ref.subquery) {
		auto child_node = CreatePlan(*ref.subquery);

		reference<LogicalOperator> node = *ref.get;

		while (!node.get().children.empty()) {
			D_ASSERT(node.get().children.size() == 1);
			if (node.get().children.size() != 1) {
				throw InternalException(
				    "Binder::CreatePlan<BoundTableFunction>: linear path expected, but found node with %d children",
				    node.get().children.size());
			}
			node = *node.get().children[0];
		}

		D_ASSERT(node.get().type == LogicalOperatorType::LOGICAL_GET);
		node.get().children.push_back(std::move(child_node));
	}
	return std::move(ref.get);
}

} // namespace sabot_sql
