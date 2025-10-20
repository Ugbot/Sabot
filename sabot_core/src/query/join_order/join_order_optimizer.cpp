#include "sabot/query/join_order_optimizer.h"
#include <algorithm>
#include <limits>
#include <chrono>

namespace sabot {
namespace query {

JoinOrderOptimizer::JoinOrderOptimizer() : stats_{}, max_joins_(10) {}

std::shared_ptr<LogicalPlan> JoinOrderOptimizer::Optimize(std::shared_ptr<LogicalPlan> plan) {
    if (!plan) {
        return nullptr;
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Only optimize if we have joins
    if (plan->GetType() != LogicalOperatorType::JOIN) {
        // No joins to optimize
        return plan;
    }
    
    // Extract joins from plan
    auto joins = ExtractJoins(plan);
    
    if (joins.size() < 2) {
        // Less than 2 joins - no optimization needed
        return plan;
    }
    
    if (joins.size() > max_joins_) {
        // Too many joins for DP - use greedy or skip
        return plan;
    }
    
    // Build join graph
    auto graph = BuildJoinGraph(plan);
    
    // Enumerate join orders using DP
    auto optimized = EnumerateJoinOrders(graph);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    stats_.joins_reordered = joins.size();
    stats_.total_time_ms = duration.count() / 1000.0;
    
    return optimized ? optimized : plan;
}

std::vector<std::shared_ptr<JoinPlan>> JoinOrderOptimizer::ExtractJoins(
    const std::shared_ptr<LogicalPlan>& plan
) {
    std::vector<std::shared_ptr<JoinPlan>> joins;
    
    // Recursively extract all join nodes
    if (plan->GetType() == LogicalOperatorType::JOIN) {
        auto join = std::dynamic_pointer_cast<JoinPlan>(plan);
        if (join) {
            joins.push_back(join);
        }
    }
    
    // Recurse into children
    for (const auto& child : plan->GetChildren()) {
        auto child_joins = ExtractJoins(child);
        joins.insert(joins.end(), child_joins.begin(), child_joins.end());
    }
    
    return joins;
}

JoinOrderOptimizer::JoinGraph JoinOrderOptimizer::BuildJoinGraph(
    const std::shared_ptr<LogicalPlan>& plan
) {
    JoinGraph graph;
    
    // Extract joins
    graph.joins = ExtractJoins(plan);
    
    // Find base tables (scans)
    std::function<void(const std::shared_ptr<LogicalPlan>&)> find_scans;
    find_scans = [&](const std::shared_ptr<LogicalPlan>& node) {
        if (node->GetType() == LogicalOperatorType::SCAN) {
            auto scan = std::dynamic_pointer_cast<ScanPlan>(node);
            if (scan) {
                graph.base_tables.push_back(node);
                // Estimate cardinality
                int64_t card = node->EstimateCardinality();
                if (card < 0) {
                    card = 10000;  // Default estimate
                }
                graph.table_cardinalities[scan->GetSourceName()] = card;
            }
        }
        
        for (const auto& child : node->GetChildren()) {
            find_scans(child);
        }
    };
    
    find_scans(plan);
    
    return graph;
}

std::shared_ptr<LogicalPlan> JoinOrderOptimizer::EnumerateJoinOrders(
    const JoinGraph& graph
) {
    if (graph.joins.size() < 2) {
        return nullptr;
    }
    
    // Simplified DP approach
    // For each subset of tables, find best way to join them
    
    // Start with best single-table scans
    std::vector<JoinNode> dp_table;
    
    for (const auto& table : graph.base_tables) {
        JoinNode node;
        node.plan = table;
        node.cardinality = table->EstimateCardinality();
        node.cost = cost_model_.ComputeScanCost(node.cardinality);
        
        auto scan = std::dynamic_pointer_cast<ScanPlan>(table);
        if (scan) {
            node.tables.push_back(scan->GetSourceName());
        }
        
        dp_table.push_back(node);
    }
    
    // For each join, try to combine existing nodes
    for (const auto& join : graph.joins) {
        // Find which nodes can be joined
        auto left = join->GetLeft();
        auto right = join->GetRight();
        
        // Simplified: just take first valid join order
        // Real DP would enumerate all combinations
        
        // Estimate cost of this join
        int64_t left_card = left->EstimateCardinality();
        int64_t right_card = right->EstimateCardinality();
        double selectivity = 0.1;  // Default
        
        int64_t output_card = cardinality_estimator_.EstimateJoin(
            left_card, right_card, selectivity
        );
        
        double join_cost = cost_model_.ComputeJoinCost(
            left_card, right_card, output_card
        );
        
        // Create new join node
        JoinNode node;
        node.plan = join;
        node.cardinality = output_card;
        node.cost = join_cost;
        
        stats_.plans_evaluated++;
    }
    
    // Return optimized plan (simplified - just return first valid order for now)
    // Real implementation would do full DP and select minimum cost
    
    return nullptr;  // Placeholder - return original plan for now
}

JoinNode JoinOrderOptimizer::FindBestJoin(
    const std::vector<std::shared_ptr<LogicalPlan>>& tables,
    const std::vector<std::shared_ptr<JoinPlan>>& available_joins
) {
    JoinNode best_node;
    double min_cost = std::numeric_limits<double>::max();
    
    // Try each applicable join
    for (const auto& join : available_joins) {
        // Check if join is applicable
        // Simplified: would need proper subset checking
        
        // Estimate cost
        int64_t left_card = join->GetLeft()->EstimateCardinality();
        int64_t right_card = join->GetRight()->EstimateCardinality();
        double selectivity = 0.1;
        
        int64_t output_card = cardinality_estimator_.EstimateJoin(
            left_card, right_card, selectivity
        );
        
        double cost = cost_model_.ComputeJoinCost(
            left_card, right_card, output_card
        );
        
        if (cost < min_cost) {
            min_cost = cost;
            best_node.plan = join;
            best_node.cardinality = output_card;
            best_node.cost = cost;
        }
    }
    
    return best_node;
}

bool JoinOrderOptimizer::JoinApplicable(
    const std::shared_ptr<JoinPlan>& join,
    const std::unordered_set<std::string>& tables
) {
    // Check if join's inputs are in the table set
    // Simplified implementation
    return true;  // Placeholder
}

std::shared_ptr<LogicalPlan> JoinOrderOptimizer::ReconstructPlan(const JoinNode& node) {
    return node.plan;
}

} // namespace query
} // namespace sabot

