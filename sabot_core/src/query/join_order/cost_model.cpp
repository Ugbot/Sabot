#include "sabot/query/join_order_optimizer.h"
#include <algorithm>

namespace sabot {
namespace query {

double CostModel::ComputeJoinCost(
    int64_t left_card,
    int64_t right_card,
    int64_t output_card,
    bool is_hash_join
) {
    if (is_hash_join) {
        // Hash join cost model
        // Cost = build hash table + probe + output
        
        double build_cost = static_cast<double>(right_card) * BUILD_COST_PER_ROW;
        double probe_cost = static_cast<double>(left_card) * PROBE_COST_PER_ROW;
        double output_cost = static_cast<double>(output_card) * OUTPUT_COST_PER_ROW;
        
        return build_cost + probe_cost + output_cost;
    } else {
        // Nested loop join cost (worse)
        double cost = static_cast<double>(left_card) * 
                     static_cast<double>(right_card) * 
                     2.0;  // Higher cost per tuple
        
        return cost;
    }
}

double CostModel::ComputeScanCost(int64_t cardinality) {
    return static_cast<double>(cardinality) * SCAN_COST_PER_ROW;
}

double CostModel::ComputeFilterCost(int64_t input_card, double selectivity) {
    // Cost = read all rows + output selected rows
    double read_cost = static_cast<double>(input_card) * FILTER_COST_PER_ROW;
    double output_card = static_cast<double>(input_card) * selectivity;
    double output_cost = output_card * OUTPUT_COST_PER_ROW;
    
    return read_cost + output_cost;
}

} // namespace query
} // namespace sabot

