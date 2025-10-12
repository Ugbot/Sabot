#include "sabot_sql/optimizer/join_order/join_node.hpp"
#include "sabot_sql/optimizer/join_order/join_order_optimizer.hpp"
#include "sabot_sql/optimizer/join_order/cost_model.hpp"

namespace sabot_sql {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	return join_cost + left.cost + right.cost;
}

} // namespace sabot_sql
