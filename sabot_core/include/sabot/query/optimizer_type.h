#pragma once

#include <string>
#include <unordered_set>
#include <cstdint>

namespace sabot {
namespace query {

/**
 * @brief Optimizer types (inspired by DuckDB's OptimizerType enum)
 * 
 * Used to enable/disable individual optimizations.
 */
enum class OptimizerType : uint32_t {
    INVALID = 0,
    
    // Expression rewriting
    EXPRESSION_REWRITER,
    CONSTANT_FOLDING,
    ARITHMETIC_SIMPLIFICATION,
    COMPARISON_SIMPLIFICATION,
    
    // Filter optimizations
    FILTER_PUSHDOWN,
    FILTER_PULLUP,
    FILTER_COMBINER,
    
    // Projection optimizations
    PROJECTION_PUSHDOWN,
    UNUSED_COLUMNS,
    
    // Join optimizations
    JOIN_ORDER,
    JOIN_FILTER_PUSHDOWN,
    BUILD_PROBE_SIDE,
    
    // Aggregation optimizations
    COMMON_AGGREGATE,
    DISTINCT_AGGREGATE,
    
    // Limit optimizations
    LIMIT_PUSHDOWN,
    TOP_N,
    
    // Other
    COMMON_SUBEXPRESSIONS,
    STATISTICS_PROPAGATION,
    
    // Streaming-specific (not in DuckDB)
    WATERMARK_PUSHDOWN,
    BACKPRESSURE_OPTIMIZATION
};

/**
 * @brief Convert optimizer type to string
 */
std::string OptimizerTypeToString(OptimizerType type);

/**
 * @brief Convert string to optimizer type
 */
OptimizerType OptimizerTypeFromString(const std::string& str);

/**
 * @brief List all available optimizers
 */
std::vector<std::string> ListAllOptimizers();

} // namespace query
} // namespace sabot

