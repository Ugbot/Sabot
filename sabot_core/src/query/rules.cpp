#include "sabot/query/optimizer.h"
#include <algorithm>

namespace sabot {
namespace query {

// FilterPushdownRule implementation

std::shared_ptr<LogicalPlan> FilterPushdownRule::Apply(
    const std::shared_ptr<LogicalPlan>& plan
) {
    // For now, return plan unchanged
    // TODO: Implement filter pushdown logic
    // - Find Filter -> Scan patterns
    // - Push filter into scan
    return plan;
}

bool FilterPushdownRule::IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const {
    // Check if this is a filter over a scan
    if (plan->GetType() != LogicalOperatorType::FILTER) {
        return false;
    }
    
    auto children = plan->GetChildren();
    if (children.empty()) {
        return false;
    }
    
    return children[0]->GetType() == LogicalOperatorType::SCAN;
}

// ProjectionPushdownRule implementation

std::shared_ptr<LogicalPlan> ProjectionPushdownRule::Apply(
    const std::shared_ptr<LogicalPlan>& plan
) {
    // For now, return plan unchanged
    // TODO: Implement projection pushdown
    return plan;
}

bool ProjectionPushdownRule::IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const {
    // Check if this is a project over a scan
    if (plan->GetType() != LogicalOperatorType::PROJECT) {
        return false;
    }
    
    auto children = plan->GetChildren();
    if (children.empty()) {
        return false;
    }
    
    return children[0]->GetType() == LogicalOperatorType::SCAN;
}

// JoinReorderingRule implementation

std::shared_ptr<LogicalPlan> JoinReorderingRule::Apply(
    const std::shared_ptr<LogicalPlan>& plan
) {
    // For now, return plan unchanged
    // TODO: Implement join reordering
    // - Collect all joins in subtree
    // - Reorder based on cardinality estimates
    // - Build left-deep tree
    return plan;
}

bool JoinReorderingRule::IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const {
    // Check if plan contains joins
    return plan->GetType() == LogicalOperatorType::JOIN;
}

// ConstantFoldingRule implementation

std::shared_ptr<LogicalPlan> ConstantFoldingRule::Apply(
    const std::shared_ptr<LogicalPlan>& plan
) {
    // For now, return plan unchanged
    // TODO: Implement constant folding
    // - Find constant expressions
    // - Evaluate at compile time
    // - Replace with literal values
    return plan;
}

bool ConstantFoldingRule::IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const {
    // Check if plan contains filters with constants
    if (plan->GetType() == LogicalOperatorType::FILTER) {
        // TODO: Check if filter has constants
        return false;
    }
    
    return false;
}

} // namespace query
} // namespace sabot

