#include "sabot/query/join_order_optimizer.h"
#include <algorithm>
#include <cmath>

namespace sabot {
namespace query {

int64_t CardinalityEstimator::EstimateJoin(
    int64_t left_card,
    int64_t right_card,
    double selectivity
) {
    // Basic join cardinality estimation
    // output = left * right * selectivity
    
    if (left_card <= 0 || right_card <= 0) {
        return 0;
    }
    
    // Clamp selectivity to reasonable range
    selectivity = std::max(0.0, std::min(1.0, selectivity));
    
    // Compute estimated cardinality
    double estimated = static_cast<double>(left_card) * 
                      static_cast<double>(right_card) * 
                      selectivity;
    
    // Avoid overflow
    return static_cast<int64_t>(std::min(estimated, 1e15));
}

double CardinalityEstimator::EstimateFilterSelectivity(const std::string& predicate) {
    // Heuristic-based selectivity estimation
    // In production, would use statistics from data sources
    
    if (predicate.empty()) {
        return 1.0;  // No filter
    }
    
    // Check for common patterns
    if (predicate.find(" = ") != std::string::npos ||
        predicate.find(" == ") != std::string::npos) {
        // Equality predicate - typically high selectivity
        return EQUALITY_SELECTIVITY;  // 0.1 (10%)
    }
    
    if (predicate.find(" LIKE ") != std::string::npos ||
        predicate.find(" like ") != std::string::npos) {
        // LIKE predicate - medium selectivity
        return LIKE_SELECTIVITY;  // 0.25 (25%)
    }
    
    if (predicate.find(" > ") != std::string::npos ||
        predicate.find(" < ") != std::string::npos ||
        predicate.find(" >= ") != std::string::npos ||
        predicate.find(" <= ") != std::string::npos ||
        predicate.find(" BETWEEN ") != std::string::npos) {
        // Range predicate - lower selectivity
        return RANGE_SELECTIVITY;  // 0.33 (33%)
    }
    
    // Default selectivity for unknown predicates
    return DEFAULT_SELECTIVITY;  // 0.1 (10%)
}

int64_t CardinalityEstimator::EstimateGroupBy(int64_t input_card, size_t num_groups) {
    if (num_groups == 0) {
        return 1;  // Global aggregation
    }
    
    // Estimate number of distinct group values
    // Simplified: assume moderate cardinality
    // Real implementation would use HyperLogLog or statistics
    
    double estimated_groups = std::min(
        static_cast<double>(input_card),
        static_cast<double>(input_card) * 0.1  // 10% unique by default
    );
    
    return static_cast<int64_t>(estimated_groups);
}

} // namespace query
} // namespace sabot

