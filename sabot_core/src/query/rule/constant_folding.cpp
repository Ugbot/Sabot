#include "sabot/query/expression_rules.h"
#include <regex>
#include <sstream>

namespace sabot {
namespace query {

bool ConstantFoldingRule::IsApplicable(const std::string& expr) const {
    // Check if expression contains arithmetic operations with constants
    return expr.find('+') != std::string::npos ||
           expr.find('-') != std::string::npos ||
           expr.find('*') != std::string::npos ||
           expr.find('/') != std::string::npos;
}

std::string ConstantFoldingRule::Rewrite(const std::string& expr) {
    if (!IsApplicable(expr)) {
        return expr;
    }
    
    // Simplified constant folding
    // Real implementation would use expression parser/evaluator
    
    // Look for simple patterns like "1 + 2"
    std::regex simple_add(R"((\d+)\s*\+\s*(\d+))");
    std::smatch match;
    
    std::string result = expr;
    
    if (std::regex_search(expr, match, simple_add)) {
        int left = std::stoi(match[1].str());
        int right = std::stoi(match[2].str());
        int sum = left + right;
        
        result = std::regex_replace(expr, simple_add, std::to_string(sum));
    }
    
    // More patterns would be added here (multiplication, etc.)
    
    return result;
}

bool ConstantFoldingRule::IsConstantExpression(const std::string& expr) const {
    // Simple check: only digits, operators, and whitespace
    std::regex constant_pattern(R"(^[\d\s\+\-\*\/\(\)\.]+$)");
    return std::regex_match(expr, constant_pattern);
}

std::string ConstantFoldingRule::EvaluateConstant(const std::string& expr) {
    // Would use expression evaluator here
    // For now, return as-is
    return expr;
}

} // namespace query
} // namespace sabot

