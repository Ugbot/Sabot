#include "sabot_sql/optimizer/join_order/join_node.hpp"

#include "sabot_sql/common/limits.hpp"
#include "sabot_sql/planner/expression/list.hpp"
#include "sabot_sql/planner/operator/list.hpp"

namespace sabot_sql {

DPJoinNode::DPJoinNode(JoinRelationSet &set) : set(set), info(nullptr), is_leaf(true), left_set(set), right_set(set) {
}

DPJoinNode::DPJoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, JoinRelationSet &left,
                       JoinRelationSet &right, double cost)
    : set(set), info(info), is_leaf(false), left_set(left), right_set(right), cost(cost) {
}

} // namespace sabot_sql
