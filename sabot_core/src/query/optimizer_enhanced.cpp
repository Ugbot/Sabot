#include "sabot/query/optimizer.h"
#include "sabot/query/filter_pushdown.h"
#include "sabot/query/optimizer_type.h"
#include <chrono>
#include <iostream>

namespace sabot {
namespace query {

/**
 * @brief Enhanced optimizer based on DuckDB's optimizer.cpp pattern
 * 
 * Sequential optimization pipeline with:
 * - Per-optimizer profiling
 * - Enable/disable controls
 * - Verification between passes
 * - Fixed-point iteration
 */
class QueryOptimizerEnhanced {
public:
    QueryOptimizerEnhanced() {
        // Initialize with enabled optimizers
        disabled_optimizers_.clear();
    }
    
    std::shared_ptr<LogicalPlan> Optimize(std::shared_ptr<LogicalPlan> plan) {
        if (!plan) {
            return nullptr;
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        
        current_plan_ = plan;
        
        // Run built-in optimizers in sequence (like DuckDB)
        RunBuiltInOptimizers();
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Update stats
        stats_.plans_optimized++;
        stats_.total_time_ms += duration.count() / 1000.0;
        stats_.avg_time_ms = stats_.total_time_ms / stats_.plans_optimized;
        
        return current_plan_;
    }
    
    void DisableOptimizer(OptimizerType type) {
        disabled_optimizers_.insert(type);
    }
    
    void EnableOptimizer(OptimizerType type) {
        disabled_optimizers_.erase(type);
    }
    
    bool OptimizerDisabled(OptimizerType type) const {
        return disabled_optimizers_.find(type) != disabled_optimizers_.end();
    }
    
    struct OptimizerStats {
        int64_t plans_optimized = 0;
        int64_t rules_applied = 0;
        double total_time_ms = 0.0;
        double avg_time_ms = 0.0;
    };
    
    OptimizerStats GetStats() const { return stats_; }
    
private:
    std::shared_ptr<LogicalPlan> current_plan_;
    std::unordered_set<OptimizerType> disabled_optimizers_;
    OptimizerStats stats_;
    
    void RunBuiltInOptimizers() {
        // Sequential optimization pipeline (based on DuckDB's order)
        
        // 1. Expression rewriting (constant folding, simplification)
        RunOptimizer(OptimizerType::CONSTANT_FOLDING, [this]() {
            // TODO: Implement constant folding
        });
        
        // 2. Filter pushdown (most important for performance)
        RunOptimizer(OptimizerType::FILTER_PUSHDOWN, [this]() {
            FilterPushdown filter_pushdown;
            current_plan_ = filter_pushdown.Rewrite(current_plan_);
            stats_.rules_applied += filter_pushdown.GetStats().filters_pushed;
        });
        
        // 3. Projection pushdown (column pruning)
        RunOptimizer(OptimizerType::PROJECTION_PUSHDOWN, [this]() {
            // TODO: Implement projection pushdown
        });
        
        // 4. Join reordering (expensive, but valuable)
        RunOptimizer(OptimizerType::JOIN_ORDER, [this]() {
            // TODO: Implement join order optimization
        });
        
        // 5. Limit pushdown (cheap and effective)
        RunOptimizer(OptimizerType::LIMIT_PUSHDOWN, [this]() {
            // TODO: Implement limit pushdown
        });
        
        // 6. Common subexpression elimination
        RunOptimizer(OptimizerType::COMMON_SUBEXPRESSIONS, [this]() {
            // TODO: Implement CSE
        });
        
        // 7. Statistics propagation (for better estimates)
        RunOptimizer(OptimizerType::STATISTICS_PROPAGATION, [this]() {
            // TODO: Implement statistics propagation
        });
    }
    
    template<typename Callback>
    void RunOptimizer(OptimizerType type, Callback callback) {
        if (OptimizerDisabled(type)) {
            return;
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        
        callback();
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Profiling output (can be disabled)
        #ifdef SABOT_OPTIMIZER_PROFILING
        std::cout << "Optimizer " << OptimizerTypeToString(type) 
                  << " took " << duration.count() << " μs" << std::endl;
        #endif
        
        // Verify plan after optimization
        if (current_plan_) {
            Verify(current_plan_);
        }
    }
    
    void Verify(std::shared_ptr<LogicalPlan> plan) {
        // Verify plan correctness
        // In debug builds, we validate the plan structure
        #ifdef DEBUG
        if (!plan->Validate()) {
            throw std::runtime_error("Invalid plan after optimization");
        }
        #endif
    }
};

// Implementation of original optimizer using enhanced version
QueryOptimizer::QueryOptimizer() {
    // Initialize rules (kept for backward compatibility)
    rules_.push_back(std::make_unique<FilterPushdownRule>());
    rules_.push_back(std::make_unique<ProjectionPushdownRule>());
    rules_.push_back(std::make_unique<JoinReorderingRule>());
    rules_.push_back(std::make_unique<ConstantFoldingRule>());
    
    for (const auto& rule : rules_) {
        rule_enabled_[rule->GetName()] = true;
    }
}

std::shared_ptr<LogicalPlan> QueryOptimizer::Optimize(
    const std::shared_ptr<LogicalPlan>& plan
) {
    // Use enhanced optimizer
    QueryOptimizerEnhanced enhanced;
    return enhanced.Optimize(plan);
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
    
    if (!rule->IsApplicable(plan)) {
        return plan;
    }
    
    auto optimized = rule->Apply(plan);
    
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
    
    // Simple equality check
    return p1->GetType() == p2->GetType();
}

std::unique_ptr<QueryOptimizer> CreateDefaultOptimizer() {
    return std::make_unique<QueryOptimizer>();
}

} // namespace query
} // namespace sabot

