#include "sabot/query/expression_rules.h"
#include <regex>

namespace sabot {
namespace query {

bool ComparisonSimplificationRule::IsApplicable(const std::string& expr) const {
    // Check for comparison operators
    return expr.find(" > ") != std::string::npos ||
           expr.find(" < ") != std::string::npos ||
           expr.find(" = ") != std::string::npos ||
           expr.find(" <> ") != std::string::npos;
}

std::string ComparisonSimplificationRule::Rewrite(const std::string& expr) {
    if (!IsApplicable(expr)) {
        return expr;
    }
    
    std::string result = expr;
    
    // Rule 1: x > x => FALSE
    std::regex self_greater(R"((\w+)\s*>\s*\1)");
    result = std::regex_replace(result, self_greater, "FALSE");
    
    // Rule 2: x < x => FALSE
    std::regex self_less(R"((\w+)\s*<\s*\1)");
    result = std::regex_replace(result, self_less, "FALSE");
    
    // Rule 3: x = x => TRUE (assuming not nullable)
    std::regex self_equal(R"((\w+)\s*=\s*\1)");
    result = std::regex_replace(result, self_equal, "TRUE");
    
    // Rule 4: x <> x => FALSE
    std::regex self_not_equal(R"((\w+)\s*<>\s*\1)");
    result = std::regex_replace(result, self_not_equal, "FALSE");
    
    return result;
}

} // namespace query
} // namespace sabot

