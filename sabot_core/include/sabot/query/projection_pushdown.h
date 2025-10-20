#pragma once

#include "logical_plan.h"
#include <memory>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <string>

namespace sabot {
namespace query {

/**
 * @brief Projection pushdown optimizer (based on DuckDB's remove_unused_columns.cpp)
 * 
 * Analyzes which columns are actually used and pushes column selection
 * early in the plan to minimize data transfer and memory usage.
 * 
 * Key benefits:
 * - Reduces data transfer between operators
 * - Lower memory footprint
 * - Faster serialization/deserialization
 * - Better cache utilization
 * 
 * Example:
 *   SELECT a FROM (SELECT a, b, c, d FROM table)
 *   => 
 *   SELECT a FROM (SELECT a FROM table)  -- Only select 'a' early
 * 
 * Performance target: <30μs per plan
 */
class ProjectionPushdown {
public:
    ProjectionPushdown();
    ~ProjectionPushdown() = default;
    
    /**
     * @brief Perform projection pushdown on plan
     * 
     * @param plan Input logical plan
     * @return Optimized plan with projections pushed down
     */
    std::shared_ptr<LogicalPlan> Rewrite(std::shared_ptr<LogicalPlan> plan);
    
    /**
     * @brief Get statistics
     */
    struct Stats {
        int64_t projections_added = 0;
        int64_t columns_eliminated = 0;
        int64_t operators_optimized = 0;
    };
    
    Stats GetStats() const { return stats_; }
    
private:
    Stats stats_;
    
    /**
     * @brief Analyze column usage in plan tree
     * 
     * Returns set of columns that are actually needed.
     */
    std::unordered_set<std::string> AnalyzeColumnUsage(const std::shared_ptr<LogicalPlan>& plan);
    
    /**
     * @brief Recursively analyze column usage
     */
    void AnalyzeNode(
        const std::shared_ptr<LogicalPlan>& node,
        std::unordered_set<std::string>& required_columns
    );
    
    /**
     * @brief Insert projection at node if beneficial
     */
    std::shared_ptr<LogicalPlan> InsertProjection(
        std::shared_ptr<LogicalPlan> node,
        const std::unordered_set<std::string>& required_columns
    );
    
    /**
     * @brief Optimize scan node
     */
    std::shared_ptr<LogicalPlan> OptimizeScan(
        std::shared_ptr<LogicalPlan> scan,
        const std::unordered_set<std::string>& required_columns
    );
    
    /**
     * @brief Optimize project node
     */
    std::shared_ptr<LogicalPlan> OptimizeProject(
        std::shared_ptr<LogicalPlan> project,
        const std::unordered_set<std::string>& required_columns
    );
    
    /**
     * @brief Optimize join node
     */
    std::shared_ptr<LogicalPlan> OptimizeJoin(
        std::shared_ptr<LogicalPlan> join,
        const std::unordered_set<std::string>& required_columns
    );
    
    /**
     * @brief Get columns referenced by operator
     */
    std::unordered_set<std::string> GetReferencedColumns(const LogicalPlan& node);
    
    /**
     * @brief Get columns produced by operator
     */
    std::unordered_set<std::string> GetProducedColumns(const LogicalPlan& node);
};

} // namespace query
} // namespace sabot

