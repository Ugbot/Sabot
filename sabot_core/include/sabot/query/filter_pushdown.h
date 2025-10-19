#pragma once

#include "logical_plan.h"
#include <memory>
#include <vector>
#include <unordered_set>
#include <string>

namespace sabot {
namespace query {

/**
 * @brief Filter pushdown optimizer (based on DuckDB's filter_pushdown.cpp)
 * 
 * Pushes filter operators closer to data sources to reduce data volume early.
 * Much more sophisticated than the 205-line Python version.
 * 
 * Key features:
 * - Push through joins (with proper side handling)
 * - Push through aggregations (when safe)
 * - Push through projections
 * - Push through windows
 * - Combine filters for better efficiency
 * 
 * Performance target: <50μs per plan (vs ~1-10ms in Python)
 */
class FilterPushdown {
public:
    FilterPushdown();
    ~FilterPushdown() = default;
    
    /**
     * @brief Perform filter pushdown on plan
     * 
     * @param plan Input logical plan
     * @return Optimized plan with filters pushed down
     */
    std::shared_ptr<LogicalPlan> Rewrite(std::shared_ptr<LogicalPlan> plan);
    
    /**
     * @brief Get statistics
     */
    struct Stats {
        int64_t filters_pushed = 0;
        int64_t filters_combined = 0;
        int64_t filters_eliminated = 0;
    };
    
    Stats GetStats() const { return stats_; }
    
private:
    Stats stats_;
    
    // Filter representation
    struct Filter {
        std::string predicate;
        std::unordered_set<std::string> referenced_columns;
        double selectivity = 0.5;  // Estimated selectivity
        
        Filter(const std::string& pred) : predicate(pred) {
            ExtractReferencedColumns();
        }
        
        void ExtractReferencedColumns();
    };
    
    // Current filters being pushed
    std::vector<Filter> filters_;
    
    /**
     * @brief Push down a filter operator
     */
    std::shared_ptr<LogicalPlan> PushdownFilter(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through scan operator
     */
    std::shared_ptr<LogicalPlan> PushdownScan(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through projection
     */
    std::shared_ptr<LogicalPlan> PushdownProjection(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through join (most complex case)
     */
    std::shared_ptr<LogicalPlan> PushdownJoin(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through inner join
     */
    std::shared_ptr<LogicalPlan> PushdownInnerJoin(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through left join
     */
    std::shared_ptr<LogicalPlan> PushdownLeftJoin(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through aggregation
     */
    std::shared_ptr<LogicalPlan> PushdownAggregate(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Push through group by
     */
    std::shared_ptr<LogicalPlan> PushdownGroupBy(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Finish pushdown (recursively process children)
     */
    std::shared_ptr<LogicalPlan> FinishPushdown(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Add remaining filters as LogicalFilter at current level
     */
    std::shared_ptr<LogicalPlan> PushFinalFilters(std::shared_ptr<LogicalPlan> op);
    
    /**
     * @brief Combine multiple filters into single predicate
     */
    std::string CombineFilters(const std::vector<Filter>& filters);
    
    /**
     * @brief Check if filter can be pushed through operator
     */
    bool CanPushThrough(const Filter& filter, const LogicalPlan& op);
    
    /**
     * @brief Split filters by which side of join they apply to
     */
    struct JoinFilterSplit {
        std::vector<Filter> left_filters;
        std::vector<Filter> right_filters;
        std::vector<Filter> join_filters;  // Filters on join condition
    };
    
    JoinFilterSplit SplitFiltersForJoin(
        const std::vector<Filter>& filters,
        const JoinPlan& join
    );
    
    /**
     * @brief Get columns available from a plan node
     */
    std::unordered_set<std::string> GetAvailableColumns(const LogicalPlan& plan);
};

} // namespace query
} // namespace sabot

