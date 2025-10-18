#pragma once

#include "logical_plan.h"
#include <memory>
#include <vector>

namespace sabot {
namespace query {

/**
 * @brief Optimization rule interface
 */
class OptimizationRule {
public:
    virtual ~OptimizationRule() = default;
    
    /**
     * @brief Apply rule to plan
     * 
     * @param plan Input plan
     * @return Optimized plan (or original if no optimization possible)
     */
    virtual std::shared_ptr<LogicalPlan> Apply(const std::shared_ptr<LogicalPlan>& plan) = 0;
    
    /**
     * @brief Get rule name
     */
    virtual std::string GetName() const = 0;
    
    /**
     * @brief Check if rule is applicable
     */
    virtual bool IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const = 0;
};

/**
 * @brief Filter pushdown rule
 * 
 * Pushes filters down to data sources for early pruning.
 * 
 * Example:
 *   Filter(amount > 1000) -> Scan(table)
 *   =>
 *   Scan(table, filter="amount > 1000")
 */
class FilterPushdownRule : public OptimizationRule {
public:
    std::shared_ptr<LogicalPlan> Apply(const std::shared_ptr<LogicalPlan>& plan) override;
    std::string GetName() const override { return "FilterPushdown"; }
    bool IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const override;
};

/**
 * @brief Projection pushdown rule
 * 
 * Pushes column selection down to minimize data movement.
 * 
 * Example:
 *   Project([a, b]) -> Scan(table)
 *   =>
 *   Scan(table, columns=[a, b])
 */
class ProjectionPushdownRule : public OptimizationRule {
public:
    std::shared_ptr<LogicalPlan> Apply(const std::shared_ptr<LogicalPlan>& plan) override;
    std::string GetName() const override { return "ProjectionPushdown"; }
    bool IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const override;
};

/**
 * @brief Join reordering rule
 * 
 * Reorders joins based on estimated cardinalities for better performance.
 * Uses left-deep tree strategy.
 */
class JoinReorderingRule : public OptimizationRule {
public:
    std::shared_ptr<LogicalPlan> Apply(const std::shared_ptr<LogicalPlan>& plan) override;
    std::string GetName() const override { return "JoinReordering"; }
    bool IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const override;
};

/**
 * @brief Constant folding rule
 * 
 * Evaluates constant expressions at optimization time.
 */
class ConstantFoldingRule : public OptimizationRule {
public:
    std::shared_ptr<LogicalPlan> Apply(const std::shared_ptr<LogicalPlan>& plan) override;
    std::string GetName() const override { return "ConstantFolding"; }
    bool IsApplicable(const std::shared_ptr<LogicalPlan>& plan) const override;
};

/**
 * @brief Query optimizer
 * 
 * Applies optimization rules to logical plans.
 * Performance target: 10-50x faster than Python implementation.
 * 
 * Uses rule-based optimization (like DuckDB):
 * 1. Filter pushdown
 * 2. Projection pushdown  
 * 3. Join reordering
 * 4. Constant folding
 * 
 * Example:
 *   QueryOptimizer optimizer;
 *   auto optimized = optimizer.Optimize(plan);
 */
class QueryOptimizer {
public:
    QueryOptimizer();
    ~QueryOptimizer() = default;
    
    /**
     * @brief Optimize logical plan
     * 
     * @param plan Input logical plan
     * @return Optimized logical plan
     * 
     * Performance: < 10ms for complex queries
     */
    std::shared_ptr<LogicalPlan> Optimize(const std::shared_ptr<LogicalPlan>& plan);
    
    /**
     * @brief Add custom optimization rule
     */
    void AddRule(std::unique_ptr<OptimizationRule> rule);
    
    /**
     * @brief Get optimization statistics
     */
    struct OptimizerStats {
        int64_t plans_optimized = 0;
        int64_t rules_applied = 0;
        double total_time_ms = 0.0;
        double avg_time_ms = 0.0;
    };
    
    OptimizerStats GetStats() const { return stats_; }
    
    /**
     * @brief Enable/disable specific rule
     */
    void EnableRule(const std::string& rule_name, bool enabled = true);
    
private:
    std::vector<std::unique_ptr<OptimizationRule>> rules_;
    OptimizerStats stats_;
    std::unordered_map<std::string, bool> rule_enabled_;
    
    /**
     * @brief Apply rules until fixed point
     */
    std::shared_ptr<LogicalPlan> ApplyRulesFixedPoint(
        const std::shared_ptr<LogicalPlan>& plan,
        int max_iterations = 10
    );
    
    /**
     * @brief Apply single rule
     */
    std::shared_ptr<LogicalPlan> ApplyRule(
        const std::shared_ptr<LogicalPlan>& plan,
        OptimizationRule* rule
    );
    
    /**
     * @brief Check if two plans are equivalent
     */
    bool PlansEqual(
        const std::shared_ptr<LogicalPlan>& p1,
        const std::shared_ptr<LogicalPlan>& p2
    ) const;
};

/**
 * @brief Create default optimizer with standard rules
 */
std::unique_ptr<QueryOptimizer> CreateDefaultOptimizer();

} // namespace query
} // namespace sabot

