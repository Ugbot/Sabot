#include "sabot/query/expression_rules.h"
#include <regex>

namespace sabot {
namespace query {

bool ArithmeticSimplificationRule::IsApplicable(const std::string& expr) const {
    // Check for arithmetic operations
    return expr.find('+') != std::string::npos ||
           expr.find('-') != std::string::npos ||
           expr.find('*') != std::string::npos;
}

std::string ArithmeticSimplificationRule::Rewrite(const std::string& expr) {
    if (!IsApplicable(expr)) {
        return expr;
    }
    
    std::string result = expr;
    
    // Rule 1: x + 0 => x
    std::regex add_zero(R"((\w+)\s*\+\s*0)");
    result = std::regex_replace(result, add_zero, "$1");
    
    // Rule 2: x * 1 => x
    std::regex mult_one(R"((\w+)\s*\*\s*1)");
    result = std::regex_replace(result, mult_one, "$1");
    
    // Rule 3: x * 0 => 0
    std::regex mult_zero(R"((\w+)\s*\*\s*0)");
    result = std::regex_replace(result, mult_zero, "0");
    
    // Rule 4: 0 + x => x
    std::regex zero_add(R"(0\s*\+\s*(\w+))");
    result = std::regex_replace(result, zero_add, "$1");
    
    // Rule 5: 1 * x => x
    std::regex one_mult(R"(1\s*\*\s*(\w+))");
    result = std::regex_replace(result, one_mult, "$1");
    
    return result;
}

} // namespace query
} // namespace sabot

