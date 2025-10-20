#include "sabot/query/projection_pushdown.h"
#include <algorithm>

namespace sabot {
namespace query {

ProjectionPushdown::ProjectionPushdown() : stats_{} {}

std::shared_ptr<LogicalPlan> ProjectionPushdown::Rewrite(std::shared_ptr<LogicalPlan> plan) {
    if (!plan) {
        return nullptr;
    }
    
    // Analyze which columns are actually needed
    auto required_columns = AnalyzeColumnUsage(plan);
    
    // Optimize based on node type
    switch (plan->GetType()) {
        case LogicalOperatorType::SCAN:
            return OptimizeScan(plan, required_columns);
        
        case LogicalOperatorType::PROJECT:
            return OptimizeProject(plan, required_columns);
        
        case LogicalOperatorType::JOIN:
            return OptimizeJoin(plan, required_columns);
        
        default:
            // For other operators, recursively process children
            auto children = plan->GetChildren();
            if (!children.empty()) {
                // Would need to reconstruct node with optimized children
                // For now, return as-is
            }
            return plan;
    }
}

std::unordered_set<std::string> ProjectionPushdown::AnalyzeColumnUsage(
    const std::shared_ptr<LogicalPlan>& plan
) {
    std::unordered_set<std::string> required;
    AnalyzeNode(plan, required);
    return required;
}

void ProjectionPushdown::AnalyzeNode(
    const std::shared_ptr<LogicalPlan>& node,
    std::unordered_set<std::string>& required_columns
) {
    if (!node) {
        return;
    }
    
    // Get columns referenced by this operator
    auto referenced = GetReferencedColumns(*node);
    required_columns.insert(referenced.begin(), referenced.end());
    
    // Recursively analyze children
    for (const auto& child : node->GetChildren()) {
        AnalyzeNode(child, required_columns);
    }
}

std::shared_ptr<LogicalPlan> ProjectionPushdown::OptimizeScan(
    std::shared_ptr<LogicalPlan> scan,
    const std::unordered_set<std::string>& required_columns
) {
    auto scan_ptr = std::dynamic_pointer_cast<ScanPlan>(scan);
    if (!scan_ptr) {
        return scan;
    }
    
    // Get all available columns from scan
    auto schema_result = scan->GetSchema();
    if (!schema_result.ok()) {
        return scan;
    }
    
    auto schema = schema_result.ValueOrDie();
    std::unordered_set<std::string> available_columns;
    for (int i = 0; i < schema->num_fields(); ++i) {
        available_columns.insert(schema->field(i)->name());
    }
    
    // Check if we're using all columns
    if (required_columns == available_columns) {
        // No optimization possible
        return scan;
    }
    
    // Create projection with only required columns
    std::vector<std::string> columns_to_select;
    for (const auto& col : required_columns) {
        if (available_columns.find(col) != available_columns.end()) {
            columns_to_select.push_back(col);
        }
    }
    
    if (columns_to_select.size() < available_columns.size()) {
        stats_.columns_eliminated += (available_columns.size() - columns_to_select.size());
        stats_.projections_added++;
        
        return std::make_shared<ProjectPlan>(scan, columns_to_select);
    }
    
    return scan;
}

std::shared_ptr<LogicalPlan> ProjectionPushdown::OptimizeProject(
    std::shared_ptr<LogicalPlan> project,
    const std::unordered_set<std::string>& required_columns
) {
    auto project_ptr = std::dynamic_pointer_cast<ProjectPlan>(project);
    if (!project_ptr) {
        return project;
    }
    
    // Get columns from this projection
    const auto& projected_cols = project_ptr->GetColumns();
    
    // Filter to only required columns
    std::vector<std::string> filtered_cols;
    for (const auto& col : projected_cols) {
        if (required_columns.find(col) != required_columns.end()) {
            filtered_cols.push_back(col);
        }
    }
    
    if (filtered_cols.size() < projected_cols.size()) {
        stats_.columns_eliminated += (projected_cols.size() - filtered_cols.size());
        stats_.operators_optimized++;
        
        // Create optimized projection
        return std::make_shared<ProjectPlan>(project_ptr->GetInput(), filtered_cols);
    }
    
    // Recursively optimize input
    auto optimized_input = Rewrite(project_ptr->GetInput());
    if (optimized_input != project_ptr->GetInput()) {
        return std::make_shared<ProjectPlan>(optimized_input, projected_cols);
    }
    
    return project;
}

std::shared_ptr<LogicalPlan> ProjectionPushdown::OptimizeJoin(
    std::shared_ptr<LogicalPlan> join,
    const std::unordered_set<std::string>& required_columns
) {
    auto join_ptr = std::dynamic_pointer_cast<JoinPlan>(join);
    if (!join_ptr) {
        return join;
    }
    
    // Split required columns by which side they come from
    auto left_cols = GetProducedColumns(*join_ptr->GetLeft());
    auto right_cols = GetProducedColumns(*join_ptr->GetRight());
    
    std::unordered_set<std::string> left_required;
    std::unordered_set<std::string> right_required;
    
    // Add join keys to required
    for (const auto& key : join_ptr->GetLeftKeys()) {
        left_required.insert(key);
    }
    for (const auto& key : join_ptr->GetRightKeys()) {
        right_required.insert(key);
    }
    
    // Split other required columns
    for (const auto& col : required_columns) {
        if (left_cols.find(col) != left_cols.end()) {
            left_required.insert(col);
        } else if (right_cols.find(col) != right_cols.end()) {
            right_required.insert(col);
        }
    }
    
    // Recursively optimize both sides
    auto optimized_left = Rewrite(join_ptr->GetLeft());
    auto optimized_right = Rewrite(join_ptr->GetRight());
    
    // Recreate join
    return std::make_shared<JoinPlan>(
        optimized_left,
        optimized_right,
        join_ptr->GetLeftKeys(),
        join_ptr->GetRightKeys(),
        join_ptr->GetJoinType()
    );
}

std::unordered_set<std::string> ProjectionPushdown::GetReferencedColumns(const LogicalPlan& node) {
    // Get columns referenced by this operator
    // For now, conservative: return all columns from schema
    auto schema_result = node.GetSchema();
    std::unordered_set<std::string> columns;
    
    if (schema_result.ok()) {
        auto schema = schema_result.ValueOrDie();
        for (int i = 0; i < schema->num_fields(); ++i) {
            columns.insert(schema->field(i)->name());
        }
    }
    
    return columns;
}

std::unordered_set<std::string> ProjectionPushdown::GetProducedColumns(const LogicalPlan& node) {
    // Get columns produced by this operator
    auto schema_result = node.GetSchema();
    std::unordered_set<std::string> columns;
    
    if (schema_result.ok()) {
        auto schema = schema_result.ValueOrDie();
        for (int i = 0; i < schema->num_fields(); ++i) {
            columns.insert(schema->field(i)->name());
        }
    }
    
    return columns;
}

} // namespace query
} // namespace sabot

