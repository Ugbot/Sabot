#include "sabot/query/filter_pushdown.h"
#include <algorithm>
#include <sstream>
#include <regex>

namespace sabot {
namespace query {

FilterPushdown::FilterPushdown() : stats_{} {}

std::shared_ptr<LogicalPlan> FilterPushdown::Rewrite(std::shared_ptr<LogicalPlan> plan) {
    if (!plan) {
        return nullptr;
    }
    
    // Dispatch based on operator type
    switch (plan->GetType()) {
        case LogicalOperatorType::FILTER:
            return PushdownFilter(plan);
        
        case LogicalOperatorType::SCAN:
            return PushdownScan(plan);
        
        case LogicalOperatorType::PROJECT:
            return PushdownProjection(plan);
        
        case LogicalOperatorType::JOIN:
            return PushdownJoin(plan);
        
        case LogicalOperatorType::GROUP_BY:
            return PushdownGroupBy(plan);
        
        case LogicalOperatorType::AGGREGATE:
            return PushdownAggregate(plan);
        
        default:
            // For other operators, just recursively process children
            return FinishPushdown(plan);
    }
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownFilter(std::shared_ptr<LogicalPlan> op) {
    // Extract filter operator
    auto filter = std::dynamic_pointer_cast<FilterPlan>(op);
    if (!filter) {
        return op;
    }
    
    // Add filter to our list
    filters_.emplace_back(filter->GetPredicate());
    
    // Recursively process input
    return Rewrite(filter->GetInput());
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownScan(std::shared_ptr<LogicalPlan> op) {
    // At scan - apply all accumulated filters
    if (filters_.empty()) {
        return op;
    }
    
    // Scan is a leaf node - wrap it with filter
    // In real implementation, we'd push filter INTO the scan operator
    // For now, create a FilterPlan wrapping the scan
    
    std::string combined = CombineFilters(filters_);
    stats_.filters_pushed += filters_.size();
    stats_.filters_combined += (filters_.size() > 1) ? 1 : 0;
    
    filters_.clear();
    
    return std::make_shared<FilterPlan>(op, combined);
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownProjection(std::shared_ptr<LogicalPlan> op) {
    auto project = std::dynamic_pointer_cast<ProjectPlan>(op);
    if (!project) {
        return op;
    }
    
    // Get projected columns
    const auto& projected_cols = project->GetColumns();
    std::unordered_set<std::string> available_cols(projected_cols.begin(), projected_cols.end());
    
    // Split filters: can push through vs. must stay above
    std::vector<Filter> push_through;
    std::vector<Filter> keep_above;
    
    for (const auto& filter : filters_) {
        // Can push if all referenced columns are in projection
        bool can_push = true;
        for (const auto& col : filter.referenced_columns) {
            if (available_cols.find(col) == available_cols.end()) {
                can_push = false;
                break;
            }
        }
        
        if (can_push) {
            push_through.push_back(filter);
        } else {
            keep_above.push_back(filter);
        }
    }
    
    // Push filters through projection
    filters_ = push_through;
    auto optimized_input = Rewrite(project->GetInput());
    
    // Recreate projection
    auto new_project = std::make_shared<ProjectPlan>(optimized_input, projected_cols);
    
    // Apply filters that couldn't be pushed
    if (!keep_above.empty()) {
        std::string combined = CombineFilters(keep_above);
        return std::make_shared<FilterPlan>(new_project, combined);
    }
    
    return new_project;
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownJoin(std::shared_ptr<LogicalPlan> op) {
    auto join = std::dynamic_pointer_cast<JoinPlan>(op);
    if (!join) {
        return op;
    }
    
    // Dispatch based on join type
    switch (join->GetJoinType()) {
        case JoinPlan::JoinType::INNER:
            return PushdownInnerJoin(op);
        
        case JoinPlan::JoinType::LEFT:
            return PushdownLeftJoin(op);
        
        default:
            // For other join types, be conservative - don't push through
            return FinishPushdown(op);
    }
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownInnerJoin(std::shared_ptr<LogicalPlan> op) {
    auto join = std::dynamic_pointer_cast<JoinPlan>(op);
    if (!join) {
        return op;
    }
    
    // Split filters by which side they apply to
    auto split = SplitFiltersForJoin(filters_, *join);
    
    // Clear current filters
    filters_.clear();
    
    // Push left filters into left side
    filters_ = split.left_filters;
    auto optimized_left = Rewrite(join->GetLeft());
    
    // Push right filters into right side
    filters_ = split.right_filters;
    auto optimized_right = Rewrite(join->GetRight());
    
    // Recreate join
    auto new_join = std::make_shared<JoinPlan>(
        optimized_left,
        optimized_right,
        join->GetLeftKeys(),
        join->GetRightKeys(),
        join->GetJoinType()
    );
    
    // Apply join filters (predicates that span both sides)
    if (!split.join_filters.empty()) {
        std::string combined = CombineFilters(split.join_filters);
        return std::make_shared<FilterPlan>(new_join, combined);
    }
    
    stats_.filters_pushed += split.left_filters.size() + split.right_filters.size();
    
    return new_join;
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownLeftJoin(std::shared_ptr<LogicalPlan> op) {
    auto join = std::dynamic_pointer_cast<JoinPlan>(op);
    if (!join) {
        return op;
    }
    
    // For left join, we can only push filters to left side
    // Pushing to right side would change semantics (turn it into inner join)
    
    auto split = SplitFiltersForJoin(filters_, *join);
    
    // Clear current filters
    filters_.clear();
    
    // Push only left filters
    filters_ = split.left_filters;
    auto optimized_left = Rewrite(join->GetLeft());
    
    // Don't push right filters - keep original right side
    auto optimized_right = join->GetRight();
    
    // Recreate join
    auto new_join = std::make_shared<JoinPlan>(
        optimized_left,
        optimized_right,
        join->GetLeftKeys(),
        join->GetRightKeys(),
        join->GetJoinType()
    );
    
    // Keep right and join filters above the join
    std::vector<Filter> filters_above;
    filters_above.insert(filters_above.end(), split.right_filters.begin(), split.right_filters.end());
    filters_above.insert(filters_above.end(), split.join_filters.begin(), split.join_filters.end());
    
    if (!filters_above.empty()) {
        std::string combined = CombineFilters(filters_above);
        return std::make_shared<FilterPlan>(new_join, combined);
    }
    
    stats_.filters_pushed += split.left_filters.size();
    
    return new_join;
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownGroupBy(std::shared_ptr<LogicalPlan> op) {
    auto group_by = std::dynamic_pointer_cast<GroupByPlan>(op);
    if (!group_by) {
        return op;
    }
    
    // Can only push filters that reference group keys (not aggregations)
    const auto& group_keys = group_by->GetGroupKeys();
    std::unordered_set<std::string> group_key_set(group_keys.begin(), group_keys.end());
    
    std::vector<Filter> push_through;
    std::vector<Filter> keep_above;
    
    for (const auto& filter : filters_) {
        // Can push if all referenced columns are group keys
        bool can_push = true;
        for (const auto& col : filter.referenced_columns) {
            if (group_key_set.find(col) == group_key_set.end()) {
                can_push = false;
                break;
            }
        }
        
        if (can_push) {
            push_through.push_back(filter);
        } else {
            keep_above.push_back(filter);
        }
    }
    
    // Push filters through
    filters_ = push_through;
    auto optimized_input = Rewrite(group_by->GetInput());
    
    // Recreate group by
    auto new_group_by = std::make_shared<GroupByPlan>(
        optimized_input,
        group_by->GetGroupKeys(),
        group_by->GetAggregations()
    );
    
    // Apply filters that couldn't be pushed
    if (!keep_above.empty()) {
        std::string combined = CombineFilters(keep_above);
        return std::make_shared<FilterPlan>(new_group_by, combined);
    }
    
    stats_.filters_pushed += push_through.size();
    
    return new_group_by;
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushdownAggregate(std::shared_ptr<LogicalPlan> op) {
    // Aggregations without grouping - can't push filters through
    // (they reference aggregate results)
    return FinishPushdown(op);
}

std::shared_ptr<LogicalPlan> FilterPushdown::FinishPushdown(std::shared_ptr<LogicalPlan> op) {
    // Recursively process children
    auto children = op->GetChildren();
    
    if (children.empty()) {
        // Leaf node - apply filters if any
        return PushFinalFilters(op);
    }
    
    // For now, just return with filters above (conservative)
    return PushFinalFilters(op);
}

std::shared_ptr<LogicalPlan> FilterPushdown::PushFinalFilters(std::shared_ptr<LogicalPlan> op) {
    if (filters_.empty()) {
        return op;
    }
    
    std::string combined = CombineFilters(filters_);
    filters_.clear();
    
    return std::make_shared<FilterPlan>(op, combined);
}

std::string FilterPushdown::CombineFilters(const std::vector<Filter>& filters) {
    if (filters.empty()) {
        return "";
    }
    
    if (filters.size() == 1) {
        return filters[0].predicate;
    }
    
    // Combine with AND
    std::stringstream ss;
    ss << "(";
    for (size_t i = 0; i < filters.size(); ++i) {
        if (i > 0) {
            ss << " AND ";
        }
        ss << "(" << filters[i].predicate << ")";
    }
    ss << ")";
    
    return ss.str();
}

FilterPushdown::JoinFilterSplit FilterPushdown::SplitFiltersForJoin(
    const std::vector<Filter>& filters,
    const JoinPlan& join
) {
    JoinFilterSplit result;
    
    // Get available columns from each side
    auto left_cols = GetAvailableColumns(*join.GetLeft());
    auto right_cols = GetAvailableColumns(*join.GetRight());
    
    for (const auto& filter : filters) {
        bool uses_left = false;
        bool uses_right = false;
        
        for (const auto& col : filter.referenced_columns) {
            if (left_cols.find(col) != left_cols.end()) {
                uses_left = true;
            }
            if (right_cols.find(col) != right_cols.end()) {
                uses_right = true;
            }
        }
        
        if (uses_left && uses_right) {
            // Predicate spans both sides - this is a join condition
            result.join_filters.push_back(filter);
        } else if (uses_left) {
            result.left_filters.push_back(filter);
        } else if (uses_right) {
            result.right_filters.push_back(filter);
        }
        // If neither, we don't know what to do - keep it as join filter
        else {
            result.join_filters.push_back(filter);
        }
    }
    
    return result;
}

std::unordered_set<std::string> FilterPushdown::GetAvailableColumns(const LogicalPlan& plan) {
    // Get schema and extract column names
    auto schema_result = plan.GetSchema();
    std::unordered_set<std::string> columns;
    
    if (schema_result.ok()) {
        auto schema = schema_result.ValueOrDie();
        for (int i = 0; i < schema->num_fields(); ++i) {
            columns.insert(schema->field(i)->name());
        }
    }
    
    return columns;
}

void FilterPushdown::Filter::ExtractReferencedColumns() {
    // Simple regex-based extraction (in real implementation, use expression parser)
    // Look for identifiers (alphanumeric + underscore)
    std::regex col_regex(R"(\b([a-zA-Z_][a-zA-Z0-9_]*)\b)");
    
    std::sregex_iterator iter(predicate.begin(), predicate.end(), col_regex);
    std::sregex_iterator end;
    
    // Filter out SQL keywords
    static const std::unordered_set<std::string> keywords = {
        "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "IS", "IN", "LIKE",
        "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END"
    };
    
    for (; iter != end; ++iter) {
        std::string col = (*iter)[1].str();
        
        // Convert to uppercase for keyword check
        std::string upper = col;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        
        if (keywords.find(upper) == keywords.end()) {
            referenced_columns.insert(col);
        }
    }
}

} // namespace query
} // namespace sabot

