#include "sabot/query/optimizer.h"
#include <chrono>
#include <iostream>

namespace sabot {
namespace query {

QueryOptimizer::QueryOptimizer() {
    // Initialize with default rules
    rules_.push_back(std::make_unique<FilterPushdownRule>());
    rules_.push_back(std::make_unique<ProjectionPushdownRule>());
    rules_.push_back(std::make_unique<JoinReorderingRule>());
    rules_.push_back(std::make_unique<ConstantFoldingRule>());
    
    // All rules enabled by default
    for (const auto& rule : rules_) {
        rule_enabled_[rule->GetName()] = true;
    }
}

std::shared_ptr<LogicalPlan> QueryOptimizer::Optimize(
    const std::shared_ptr<LogicalPlan>& plan
) {
    if (!plan) {
        return nullptr;
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Apply rules until fixed point
    auto optimized = ApplyRulesFixedPoint(plan);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    // Update stats
    stats_.plans_optimized++;
    stats_.total_time_ms += duration.count() / 1000.0;
    stats_.avg_time_ms = stats_.total_time_ms / stats_.plans_optimized;
    
    return optimized;
}

void QueryOptimizer::AddRule(std::unique_ptr<OptimizationRule> rule) {
    std::string rule_name = rule->GetName();
    rule_enabled_[rule_name] = true;
    rules_.push_back(std::move(rule));
}

void QueryOptimizer::EnableRule(const std::string& rule_name, bool enabled) {
    rule_enabled_[rule_name] = enabled;
}

std::shared_ptr<LogicalPlan> QueryOptimizer::ApplyRulesFixedPoint(
    const std::shared_ptr<LogicalPlan>& plan,
    int max_iterations
) {
    std::shared_ptr<LogicalPlan> current = plan;
    
    for (int iteration = 0; iteration < max_iterations; ++iteration) {
        std::shared_ptr<LogicalPlan> previous = current;
        
        // Apply each enabled rule
        for (const auto& rule : rules_) {
            if (rule_enabled_[rule->GetName()]) {
                current = ApplyRule(current, rule.get());
            }
        }
        
        // Check if we've reached fixed point
        if (PlansEqual(current, previous)) {
            break;
        }
    }
    
    return current;
}

std::shared_ptr<LogicalPlan> QueryOptimizer::ApplyRule(
    const std::shared_ptr<LogicalPlan>& plan,
    OptimizationRule* rule
) {
    if (!rule || !plan) {
        return plan;
    }
    
    // Check if rule is applicable
    if (!rule->IsApplicable(plan)) {
        return plan;
    }
    
    // Apply rule
    auto optimized = rule->Apply(plan);
    
    // Update stats if rule made changes
    if (!PlansEqual(plan, optimized)) {
        stats_.rules_applied++;
    }
    
    return optimized;
}

bool QueryOptimizer::PlansEqual(
    const std::shared_ptr<LogicalPlan>& p1,
    const std::shared_ptr<LogicalPlan>& p2
) const {
    if (!p1 && !p2) return true;
    if (!p1 || !p2) return false;
    
    // Simple equality check based on type
    // TODO: Implement deeper structural equality
    return p1->GetType() == p2->GetType();
}

std::unique_ptr<QueryOptimizer> CreateDefaultOptimizer() {
    return std::make_unique<QueryOptimizer>();
}

} // namespace query
} // namespace sabot

