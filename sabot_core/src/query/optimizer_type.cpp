#include "sabot/query/optimizer_type.h"
#include <stdexcept>

namespace sabot {
namespace query {

std::string OptimizerTypeToString(OptimizerType type) {
    switch (type) {
        case OptimizerType::INVALID: return "INVALID";
        case OptimizerType::EXPRESSION_REWRITER: return "EXPRESSION_REWRITER";
        case OptimizerType::CONSTANT_FOLDING: return "CONSTANT_FOLDING";
        case OptimizerType::ARITHMETIC_SIMPLIFICATION: return "ARITHMETIC_SIMPLIFICATION";
        case OptimizerType::COMPARISON_SIMPLIFICATION: return "COMPARISON_SIMPLIFICATION";
        case OptimizerType::FILTER_PUSHDOWN: return "FILTER_PUSHDOWN";
        case OptimizerType::FILTER_PULLUP: return "FILTER_PULLUP";
        case OptimizerType::FILTER_COMBINER: return "FILTER_COMBINER";
        case OptimizerType::PROJECTION_PUSHDOWN: return "PROJECTION_PUSHDOWN";
        case OptimizerType::UNUSED_COLUMNS: return "UNUSED_COLUMNS";
        case OptimizerType::JOIN_ORDER: return "JOIN_ORDER";
        case OptimizerType::JOIN_FILTER_PUSHDOWN: return "JOIN_FILTER_PUSHDOWN";
        case OptimizerType::BUILD_PROBE_SIDE: return "BUILD_PROBE_SIDE";
        case OptimizerType::COMMON_AGGREGATE: return "COMMON_AGGREGATE";
        case OptimizerType::DISTINCT_AGGREGATE: return "DISTINCT_AGGREGATE";
        case OptimizerType::LIMIT_PUSHDOWN: return "LIMIT_PUSHDOWN";
        case OptimizerType::TOP_N: return "TOP_N";
        case OptimizerType::COMMON_SUBEXPRESSIONS: return "COMMON_SUBEXPRESSIONS";
        case OptimizerType::STATISTICS_PROPAGATION: return "STATISTICS_PROPAGATION";
        case OptimizerType::WATERMARK_PUSHDOWN: return "WATERMARK_PUSHDOWN";
        case OptimizerType::BACKPRESSURE_OPTIMIZATION: return "BACKPRESSURE_OPTIMIZATION";
        default: return "UNKNOWN";
    }
}

OptimizerType OptimizerTypeFromString(const std::string& str) {
    if (str == "EXPRESSION_REWRITER") return OptimizerType::EXPRESSION_REWRITER;
    if (str == "CONSTANT_FOLDING") return OptimizerType::CONSTANT_FOLDING;
    if (str == "ARITHMETIC_SIMPLIFICATION") return OptimizerType::ARITHMETIC_SIMPLIFICATION;
    if (str == "COMPARISON_SIMPLIFICATION") return OptimizerType::COMPARISON_SIMPLIFICATION;
    if (str == "FILTER_PUSHDOWN") return OptimizerType::FILTER_PUSHDOWN;
    if (str == "FILTER_PULLUP") return OptimizerType::FILTER_PULLUP;
    if (str == "FILTER_COMBINER") return OptimizerType::FILTER_COMBINER;
    if (str == "PROJECTION_PUSHDOWN") return OptimizerType::PROJECTION_PUSHDOWN;
    if (str == "UNUSED_COLUMNS") return OptimizerType::UNUSED_COLUMNS;
    if (str == "JOIN_ORDER") return OptimizerType::JOIN_ORDER;
    if (str == "JOIN_FILTER_PUSHDOWN") return OptimizerType::JOIN_FILTER_PUSHDOWN;
    if (str == "BUILD_PROBE_SIDE") return OptimizerType::BUILD_PROBE_SIDE;
    if (str == "COMMON_AGGREGATE") return OptimizerType::COMMON_AGGREGATE;
    if (str == "DISTINCT_AGGREGATE") return OptimizerType::DISTINCT_AGGREGATE;
    if (str == "LIMIT_PUSHDOWN") return OptimizerType::LIMIT_PUSHDOWN;
    if (str == "TOP_N") return OptimizerType::TOP_N;
    if (str == "COMMON_SUBEXPRESSIONS") return OptimizerType::COMMON_SUBEXPRESSIONS;
    if (str == "STATISTICS_PROPAGATION") return OptimizerType::STATISTICS_PROPAGATION;
    if (str == "WATERMARK_PUSHDOWN") return OptimizerType::WATERMARK_PUSHDOWN;
    if (str == "BACKPRESSURE_OPTIMIZATION") return OptimizerType::BACKPRESSURE_OPTIMIZATION;
    
    throw std::runtime_error("Unknown optimizer type: " + str);
}

std::vector<std::string> ListAllOptimizers() {
    return {
        "EXPRESSION_REWRITER",
        "CONSTANT_FOLDING",
        "ARITHMETIC_SIMPLIFICATION",
        "COMPARISON_SIMPLIFICATION",
        "FILTER_PUSHDOWN",
        "FILTER_PULLUP",
        "FILTER_COMBINER",
        "PROJECTION_PUSHDOWN",
        "UNUSED_COLUMNS",
        "JOIN_ORDER",
        "JOIN_FILTER_PUSHDOWN",
        "BUILD_PROBE_SIDE",
        "COMMON_AGGREGATE",
        "DISTINCT_AGGREGATE",
        "LIMIT_PUSHDOWN",
        "TOP_N",
        "COMMON_SUBEXPRESSIONS",
        "STATISTICS_PROPAGATION",
        "WATERMARK_PUSHDOWN",
        "BACKPRESSURE_OPTIMIZATION"
    };
}

} // namespace query
} // namespace sabot

