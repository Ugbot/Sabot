#include "sabot/query/expression_rules.h"
#include <regex>

namespace sabot {
namespace query {

bool ConjunctionSimplificationRule::IsApplicable(const std::string& expr) const {
    // Check for AND/OR
    return expr.find(" AND ") != std::string::npos ||
           expr.find(" OR ") != std::string::npos ||
           expr.find(" and ") != std::string::npos ||
           expr.find(" or ") != std::string::npos;
}

std::string ConjunctionSimplificationRule::Rewrite(const std::string& expr) {
    if (!IsApplicable(expr)) {
        return expr;
    }
    
    std::string result = expr;
    
    // Rule 1: x AND TRUE => x
    std::regex and_true(R"((.+?)\s+AND\s+TRUE)", std::regex::icase);
    result = std::regex_replace(result, and_true, "$1");
    
    // Rule 2: TRUE AND x => x
    std::regex true_and(R"(TRUE\s+AND\s+(.+?))", std::regex::icase);
    result = std::regex_replace(result, true_and, "$1");
    
    // Rule 3: x OR FALSE => x
    std::regex or_false(R"((.+?)\s+OR\s+FALSE)", std::regex::icase);
    result = std::regex_replace(result, or_false, "$1");
    
    // Rule 4: FALSE OR x => x
    std::regex false_or(R"(FALSE\s+OR\s+(.+?))", std::regex::icase);
    result = std::regex_replace(result, false_or, "$1");
    
    // Rule 5: x AND FALSE => FALSE
    std::regex and_false(R"(.+?\s+AND\s+FALSE)", std::regex::icase);
    if (std::regex_match(result, and_false)) {
        return "FALSE";
    }
    
    // Rule 6: x OR TRUE => TRUE
    std::regex or_true(R"(.+?\s+OR\s+TRUE)", std::regex::icase);
    if (std::regex_match(result, or_true)) {
        return "TRUE";
    }
    
    return result;
}

} // namespace query
} // namespace sabot

