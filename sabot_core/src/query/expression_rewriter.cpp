#include "sabot/query/expression_rules.h"
#include <algorithm>

namespace sabot {
namespace query {

ExpressionRewriter::ExpressionRewriter() : stats_{} {
    // Initialize with default rules
    rules_.push_back(std::make_unique<ConstantFoldingRule>());
    rules_.push_back(std::make_unique<ArithmeticSimplificationRule>());
    rules_.push_back(std::make_unique<ComparisonSimplificationRule>());
    rules_.push_back(std::make_unique<ConjunctionSimplificationRule>());
}

std::string ExpressionRewriter::Rewrite(const std::string& expr) {
    std::string current = expr;
    std::string previous;
    
    // Apply rules until fixed point
    int max_iterations = 10;
    for (int i = 0; i < max_iterations; ++i) {
        previous = current;
        
        // Apply each rule
        for (const auto& rule : rules_) {
            if (rule->IsApplicable(current)) {
                std::string rewritten = rule->Rewrite(current);
                if (rewritten != current) {
                    current = rewritten;
                    stats_.rules_applied++;
                }
            }
        }
        
        // Check for fixed point
        if (current == previous) {
            break;
        }
    }
    
    if (current != expr) {
        stats_.expressions_rewritten++;
    }
    
    return current;
}

void ExpressionRewriter::AddRule(std::unique_ptr<ExpressionRule> rule) {
    rules_.push_back(std::move(rule));
}

} // namespace query
} // namespace sabot

